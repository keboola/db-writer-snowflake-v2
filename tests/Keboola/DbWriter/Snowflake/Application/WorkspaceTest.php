<?php
namespace Keboola\DbWriter\Snowflake\Application;

use Keboola\DbWriter\Snowflake\Logger\Logger;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;
use Keboola\StorageApi\Workspaces;
use Monolog\Handler\NullHandler;
use Symfony\Component\Yaml\Yaml;

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

        return $workspace['id'];
    }

    public function testConnect()
    {
        $workspaceId = $this->prepareWorkspace();

        $this->logger->setHandlers([new NullHandler()]);
        $config = $this->initConfig($workspaceId);

        $result = $this->application->run('testConnection', $config);

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    private function initConfig($workspaceId, $tablesWhere = [])
    {
        $yaml = new Yaml();
        $config = $yaml->parse(file_get_contents($this->dataDir . '/incremental/config.yml'));

        $config['parameters']['workspaceId'] = $workspaceId;
        $config['parameters']['data_dir'] = $this->dataDir . '/incremental/';

        $config['storage'] = ['input' => ['tables' => []]];
        foreach ($config['parameters']['tables'] as $key => $table)
        {
            $mappingTable = [
                'source' => 'in.c-test-wr-db-snowflake' . '.' . $table['tableId'],
                'destination' => $table['tableId'],
                'columns' => array_map(
                    function($column) {
                        return $column['name'];
                    },
                    array_filter(
                        $table['items'],
                        function($column) {
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
