<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 27/10/16
 * Time: 17:20
 */

namespace Keboola\DbWriter\Writer\Snowflake\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Snowflake\Test\BaseTest;
use Keboola\StorageApi\Client;
use Symfony\Component\Process\Process;

class FunctionalTest extends BaseTest
{
    const DRIVER = 'Snowflake';

    protected $dataDir = ROOT_PATH . 'tests/data/functional';

    protected $tmpRunDir;

    /**
     * @var Client
     */
    private $storageApi;

    public function setUp()
    {
        // init configuration
        $this->tmpRunDir = '/tmp/' . uniqid('wr-db-snowflake_');
        mkdir($this->tmpRunDir . '/in/tables/', 0777, true);
        $config = $this->initConfig();

        // cleanup KBC storage
        $this->storageApi = new Client([
            'token' => getenv('KBC_TOKEN'),
            'url' => getenv('KBC_URL'),
        ]);

        $bucketId = 'in.c-test-wr-db-snowflake';
        if ($this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->dropBucket($bucketId, ['force' => true]);
        }

        $this->storageApi->createBucket('test-wr-db-snowflake', 'in');

        foreach ($config['parameters']['tables'] as $table) {
            $tableName = trim(str_replace($bucketId, '', $table['tableId']), '.');

            $this->storageApi->createTableAsync(
                $bucketId,
                $tableName,
                new CsvFile($this->dataDir . '/in/tables/' . $tableName . '.csv')
            );
        }
    }

    public function testRun()
    {
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpRunDir . ' 2>&1', null, null, null, 180);
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), 'Output: ' . $process->getOutput());
    }

    public function testTestConnection()
    {
        $this->initConfig(function ($config) {
            $config['action'] = 'testConnection';
            return $config;
        });

        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpRunDir . ' 2>&1', null, null, null, 180);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $data = json_decode($process->getOutput(), true);

        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function testUserException()
    {
        $this->initConfig(function ($config) {
            $config['parameters']['tables'][0]['items'][1]['type'] = 'int';
            return $config;
        });

        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpRunDir . ' 2>&1', null, null, null, 180);
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
    }

    private function initConfig(callable $callback = null)
    {
        $dstConfigPath = $this->tmpRunDir . '/config.json';
        $config = json_decode(file_get_contents($this->dataDir . '/config.json'), true);

        $config['parameters']['db']['user'] = $this->getEnv(self::DRIVER, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv(self::DRIVER, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(self::DRIVER, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(self::DRIVER, 'DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv(self::DRIVER, 'DB_SCHEMA');
        $config['parameters']['db']['warehouse'] = $this->getEnv(self::DRIVER, 'DB_WAREHOUSE');

        if ($callback !== null) {
            $config = $callback($config);
        }

        $config['storage'] = ['input' => ['tables' => [], 'files' => []]];
        foreach ($config['parameters']['tables'] as $key => $table) {
            $tableId = sprintf("in.c-test-wr-db-snowflake.%s", $table['tableId']);

            $config['parameters']['tables'][$key]['tableId'] = $tableId;

            $config['storage']['input']['tables'][] = [
                'source' => $tableId,
                'destination' => sprintf("%s.csv", $tableId),
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
        }

        @unlink($dstConfigPath);
        file_put_contents($dstConfigPath, json_encode($config));

        return $config;
    }
}
