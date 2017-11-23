<?php
namespace Keboola\DbWriter\Snowflake\Application;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Snowflake\Writer;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Monolog\Handler\NullHandler;

class WorkspaceTest extends BaseTest
{
    /**
     * @var Workspace
     */
    private $application;

    public function setUp()
    {
        parent::setUp();

        $this->application = new Workspace($this->storageApi, $this->logger);
    }

    private function prepareWorkspace()
    {
        $componentId = getenv('KBC_COMPONENTID');

        $components = new Components($this->storageApi);
        $workspaces = new Workspaces($this->storageApi);

        $configurations = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );

        // cleanup
        foreach ($configurations as $configuration) {
            if ($configuration['description'] === BaseTest::CONFIGURATION_DESCRIPTION) {
                $listOptions = new ListConfigurationWorkspacesOptions();
                $listOptions->setComponentId($componentId)
                    ->setConfigurationId($configuration['id']);

                foreach ($components->listConfigurationWorkspaces($listOptions) as $workspace) {
                    $workspaces->deleteWorkspace($workspace['id']);
                }

                $components->deleteConfiguration($componentId, $configuration['id']);
            }
        }

        // create configuration and workspace
        $configuration = new Configuration();
        $configuration->setDescription(BaseTest::CONFIGURATION_DESCRIPTION)
            ->setComponentId($componentId)
            ->setName(BaseTest::CONFIGURATION_DESCRIPTION);

        $configuration = $components->addConfiguration($configuration);
        $workspace = $components->createConfigurationWorkspace($componentId, $configuration['id']);

        return $workspace;
    }

    public function testConnect()
    {
        $workspace = $this->prepareWorkspace();

        $this->logger->setHandlers([new NullHandler()]);
        $config = $this->initConfig($workspace['id']);

        $result = $this->application->run('testConnection', $config);

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRun()
    {
        $this->prepareSapiTables();
        $workspace = $this->prepareWorkspace();

        $writer = new Writer($workspace['connection'], $this->logger);

        // first run
        $whereTables = [
            'simple' => [
                'where_column' => 'glasses',
                'where_values' => ['no', 'sometimes'],
            ]
        ];

        $config = $this->initConfig($workspace['id'], $whereTables);

        foreach ($config['storage']['input']['tables'] as $table) {
            $this->assertFalse($writer->tableExists($table['destination']));
        }

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

        $config = $this->initConfig($workspace['id'], $whereTables, true);

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

    private function initConfig($workspaceId, $tablesWhere = [], $incremental = false)
    {
        $config = json_decode(file_get_contents($this->dataDir . '/incremental/config.json'), true);

        $config['parameters']['workspaceId'] = $workspaceId;
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
}
