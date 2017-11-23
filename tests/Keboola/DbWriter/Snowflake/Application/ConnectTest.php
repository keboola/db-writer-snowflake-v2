<?php
namespace Keboola\DbWriter\Snowflake\Application;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Snowflake\Writer;
use Keboola\DbWriter\Writer\SnowflakeTest;
use Monolog\Handler\NullHandler;

class ConnectTest extends BaseTest
{
    /**
     * @var Connect
     */
    private $application;

    public function setUp()
    {
        parent::setUp();

        $this->application = new Connect($this->storageApi, $this->logger);
    }

    public function testConnect()
    {
        $this->logger->setHandlers([new NullHandler()]);
        $config = $this->initConfig();

        $result = $this->application->run('testConnection', $config);

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRun()
    {
        $this->prepareSapiTables();

        $config = $this->initConfig();
        $writer = new Writer($config['parameters']['db'], $this->logger);

        // first run
        $whereTables = [
            'simple' => [
                'where_column' => 'glasses',
                'where_values' => ['no', 'sometimes'],
            ]
        ];

        $config = $this->initConfig($whereTables);

        $result = $this->application->run('run', $config);

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);

        foreach ($config['parameters']['tables'] as $table) {
            $this->assertTrue($writer->tableExists($table['dbName']));

            $res = $writer->getConnection()->fetchAll(sprintf('SELECT * FROM "%s" ORDER BY "id" ASC', $table['dbName']));

            $tableMetadata = $this->storageApi->getTable($table['tableId']);

            $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
            $csv = new CsvFile($resFilename);
            $csv->writeRow($tableMetadata['columns']);
            foreach ($res as $row) {
                $csv->writeRow($row);
            }

            $file = new CsvFile($this->dataDir . '/incremental/in/tables/' . $table['dbName'] . '_filtered.csv');
            $this->assertFileEquals((string) $file, (string) $csv);
        }

        foreach ($config['parameters']['tables'] as $table) {
            $this->storageApi->writeTableAsync(
                $table['tableId'],
                new CsvFile($this->dataDir . '/incremental/in/tables/' . $table['dbName'] . '_increment.csv'),
                [
                    'incremental' => true,
                ]
            );
        }

        // second run - increment
        $whereTables = [
            'simple' => [
                'where_column' => 'id',
                'where_values' => ['3', '4', '5'],
            ]
        ];

        $config = $this->initConfig($whereTables, true);

        $result = $this->application->run('run', $config);

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);

        foreach ($config['parameters']['tables'] as $table) {
            $this->assertTrue($writer->tableExists($table['dbName']));

            $res = $writer->getConnection()->fetchAll(sprintf('SELECT * FROM "%s" ORDER BY "id" ASC', $table['dbName']));

            $tableMetadata = $this->storageApi->getTable($table['tableId']);

            $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
            $csv = new CsvFile($resFilename);
            $csv->writeRow($tableMetadata['columns']);
            foreach ($res as $row) {
                $csv->writeRow($row);
            }

            $file = new CsvFile($this->dataDir . '/incremental/in/tables/' . $table['dbName'] . '_merged.csv');
            $this->assertFileEquals((string) $file, (string) $csv);
        }

        // null test - age column is nullable
        $res = $writer->getConnection()->fetchAll(sprintf('SELECT COUNT(*) as "count" FROM "%s" WHERE "age" IS NULL', $table['dbName']));
        $this->assertEquals(4, reset($res)['count']);

        $res = $writer->getConnection()->fetchAll(sprintf('SELECT COUNT(*) as "count" FROM "%s" WHERE "age" IS NOT NULL', $table['dbName']));
        $this->assertEquals(2, reset($res)['count']);
    }

    private function initConfig($tablesWhere = [], $incremental = false)
    {
        $driver = SnowflakeTest::DRIVER;
        $config = json_decode(file_get_contents($this->dataDir . '/incremental/config.json'), true);

        $config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv($driver, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv($driver, 'DB_SCHEMA');
        $config['parameters']['db']['warehouse'] = $this->getEnv($driver, 'DB_WAREHOUSE');
        $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];

        $config['parameters']['data_dir'] = $this->dataDir . '/incremental/';

        $config['storage'] = ['input' => ['tables' => [], 'files' => []]];
        foreach ($config['parameters']['tables'] as $key => $table) {
            $config['parameters']['tables'][$key]['tableId'] = 'in.c-test-wr-db-snowflake' . '.' . $table['tableId'];
            $config['parameters']['tables'][$key]['incremental'] = (bool) $incremental;

            $mappingTable = [
                'source' => 'in.c-test-wr-db-snowflake' . '.' . $table['tableId'],
                'destination' => $table['tableId'],
                'columns' => array_map(
                    function ($column) {
                        return $column['name'];
                    },
                    array_filter(
                        $table['items'],
                        function ($column) {
                            return $column['type'] !== 'IGNORE';
                        }
                    )
                )
            ];

            if (isset($tablesWhere[$table['tableId']])) {
                $mappingTable['where_column'] = $tablesWhere[$table['tableId']]['where_column'];
                $mappingTable['where_values'] = $tablesWhere[$table['tableId']]['where_values'];
            }

            $config['storage']['input']['tables'][] = $mappingTable;
        }

        return $config;
    }

    protected function getEnv($driver, $suffix, $required = false)
    {
        $env = strtoupper($driver) . '_' . $suffix;
        if ($required) {
            if (false === getenv($env)) {
                throw new \Exception($env . " environment variable must be set.");
            }
        }
        return getenv($env);
    }
}
