<?php
namespace Keboola\DbWriter\Snowflake\Application;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Snowflake\Configuration\ConnectDefinition;
use Keboola\DbWriter\Snowflake\Exception\UserException;
use Keboola\DbWriter\Snowflake\Exception\ApplicationException;
use Keboola\DbWriter\Snowflake\Logger\Logger;
use Keboola\DbWriter\Snowflake\StorageApi\DataLoader;
use Keboola\DbWriter\Snowflake\Writer;
use Keboola\StorageApi\Client;

class Connect extends Base
{
    public function __construct(Client $sapiClient, Logger $logger)
    {
        parent::__construct($sapiClient, $logger, new ConnectDefinition());
    }

    protected function testConnectionAction(array $config, array $mapping)
    {
        try {
            $writer = new Writer($config['db'], $this->logger);
            $writer->testConnection();
        } catch (\Exception $e) {
            throw new UserException(sprintf("Connection failed: '%s'", $e->getMessage()), 0, $e);
        }

        return [
            'status' => 'success',
        ];
    }

    /**
     * @param array $mapping
     * @param $dataDir
     * @throws ApplicationException
     */
    private function loadInputData(array $mapping, $dataDir)
    {
        try {
            $loader = new DataLoader(
                $this->sapiClient,
                $this->logger,
                $dataDir,
                $mapping,
                getenv('KBC_COMPONENTID')
            );

            $loader->loadInputData();
        } catch (\InvalidArgumentException $e) {
            throw new ApplicationException($e->getMessage());
        }
    }

    protected function runAction(array $config, array $mapping)
    {
        $uploaded = [];

        $tables = $this->filterTables($config['tables']);

        // prepare input mapping - download from tables from KBC)
        $mapping['input']['tables'] = array_filter($mapping['input']['tables'], function ($table) use ($tables) {
            $export = false;

            foreach ($tables as $config) {
                if ($config['tableId'] === $table['source']) {
                    $export = true;
                }
            }

            return $export;
        });

        $this->loadInputData($mapping, "/tmp/mapping");

        // upload tables
        $dataDir = new \SplFileInfo("/tmp/mapping/in/tables/");

        $writer = new Writer($config['db'], $this->logger);
        foreach ($tables as $table) {
            $manifest = $this->getManifest($table['tableId'], $dataDir);

            $targetTableName = $table['dbName'];
            if ($table['incremental']) {
                $table['dbName'] = $writer->generateTmpName($table['dbName']);
            }
            $table['items'] = $this->reorderColumns($manifest['columns'], $table['items']);

            try {
                $writer->drop($table['dbName']);
                $writer->create($table);
                $writer->writeFromS3($manifest['s3'], $table);

                if ($table['incremental']) {
                    // create target table if not exists
                    if (!$writer->tableExists($targetTableName)) {
                        $destinationTable = $table;
                        $destinationTable['dbName'] = $targetTableName;
                        $destinationTable['incremental'] = false;
                        $writer->create($destinationTable);
                    }
                    $writer->upsert($table, $targetTableName);
                }
            } catch (UserException $e) {
                throw $e;
            } catch (\Exception $e) {
                throw new ApplicationException($e->getMessage(), 2, $e, ["trace" => $e->getTraceAsString()]);
            }

            $uploaded[] = $table['tableId'];
        }

        return [
            'status' => 'success',
            'uploaded' => $uploaded
        ];
    }

    private function getManifest($tableId, \SplFileInfo $directory)
    {
        return json_decode(file_get_contents($directory . '/' . $tableId . ".csv.manifest"), true);
    }

    protected function reorderColumns($columns, $items)
    {
        $reordered = [];
        foreach ($columns as $manifestCol) {
            foreach ($items as $item) {
                if ($manifestCol == $item['name']) {
                    $reordered[] = $item;
                }
            }
        }
        return $reordered;
    }

    public function writeFull($csv, $tableConfig, Writer $writer)
    {
        $writer->drop($tableConfig['dbName']);
        $writer->create($tableConfig);
        $writer->write($csv, $tableConfig);
    }


    public function writeIncremental($csv, $tableConfig, Writer $writer)
    {
        // write to staging table
        $stageTable = $tableConfig;
        $stageTable['dbName'] = $writer->generateTmpName($tableConfig['dbName']);

        $writer->drop($stageTable['dbName']);
        $writer->create($stageTable);
        $writer->write($csv, $stageTable);

        // create destination table if not exists
        $dstTableExists = $writer->tableExists($tableConfig['dbName']);
        if (!$dstTableExists) {
            $writer->create($tableConfig);
        }
        $writer->validateTable($tableConfig);

        // upsert from staging to destination table
        $writer->upsert($stageTable, $tableConfig['dbName']);
    }

    protected function getInputCsv($tableId, \SplFileInfo $directory)
    {
        return new CsvFile($directory . '/' . $tableId . ".csv");
    }
}
