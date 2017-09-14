<?php
namespace Keboola\DbWriter\Snowflake\Application;

use Keboola\DbWriter\Writer\SnowflakeTest;
use Monolog\Handler\NullHandler;
use Symfony\Component\Yaml\Yaml;

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

    private function initConfig($tablesWhere = [])
    {
        $driver = SnowflakeTest::DRIVER;
        $yaml = new Yaml();
        $config = $yaml->parse(file_get_contents($this->dataDir . '/incremental/config.yml'));

        $config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv($driver, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv($driver, 'DB_SCHEMA');
        $config['parameters']['db']['warehouse'] = $this->getEnv($driver, 'DB_WAREHOUSE');
        $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];

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
