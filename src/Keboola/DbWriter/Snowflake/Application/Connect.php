<?php
namespace Keboola\DbWriter\Snowflake\Application;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Snowflake\Configuration\ConnectDefinition;
use Keboola\DbWriter\Snowflake\Configuration\WorkspaceDefinition;
use Keboola\DbWriter\Snowflake\Writer;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Symfony\Component\Yaml\Yaml;

class Connect extends Base
{
    private $writer;

    public function __construct(Client $sapiClient, Logger $logger)
    {
        parent::__construct($sapiClient, $logger, new ConnectDefinition());
    }

    private function initWriter($params)
    {
        $this->writer = new Writer($params['db'], $this->logger);
    }

    protected function testConnectionAction(array $config)
    {
        try {
            $writer = new Writer($config['db'], $this->logger);
            $writer->testConnection();
        } catch (\Exception $e) {
            throw new UserException(sprintf("Connection failed: '%s'", $e->getMessage()), 0, $e);
        }

        return json_encode([
            'status' => 'success',
        ]);
    }

    protected function testWorkspaceAction(array $config)
    {
        // TODO: Implement testWorkspaceAction() method.
    }

    protected function runAction(array $config)
    {
        $uploaded = [];
        $tables = array_filter($this['parameters']['tables'], function ($table) {
            return ($table['export']);
        });

        /** @var Writer $writer */
        $writer = $this['writer'];
        foreach ($tables as $table) {
            if (!$writer->isTableValid($table)) {
                continue;
            }
            var_dump($table);
            $manifest = $this->getManifest($table['tableId']);
            var_dump($manifest);
            $targetTableName = $table['dbName'];
            if ($table['incremental']) {
                $table['dbName'] = $writer->generateTmpName($table['dbName']);
            }
            $table['items'] = $this->reorderColumns($manifest['columns'], $table['items']);

            if (empty($table['items'])) {
                continue;
            }

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
            } catch (Exception $e) {
                throw new UserException($e->getMessage(), 0, $e, ["trace" => $e->getTraceAsString()]);
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

    private function getManifest($tableId)
    {
        return (new Yaml())->parse(
            file_get_contents(
                $this['parameters']['data_dir'] . "/in/tables/" . $tableId . ".csv.manifest"
            )
        );
    }

    protected function reorderColumns(CsvFile $file, $items)
    {
        $reordered = [];
        foreach ($file->getHeader() as $manifestCol) {
            foreach ($items as $item) {
                if ($manifestCol == $item['name']) {
                    $reordered[] = $item;
                }
            }
        }
        return $reordered;
    }

    public function writeFull($csv, $tableConfig)
    {
        /** @var WriterInterface $writer */
        $writer = $this['writer'];

        $writer->drop($tableConfig['dbName']);
        $writer->create($tableConfig);
        $writer->write($csv, $tableConfig);
    }


    public function writeIncremental($csv, $tableConfig)
    {
        /** @var WriterInterface $writer */
        $writer = $this['writer'];

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

    protected function getInputCsv($tableId)
    {
        return new CsvFile($this['parameters']['data_dir'] . "/in/tables/" . $tableId . ".csv");
    }
}