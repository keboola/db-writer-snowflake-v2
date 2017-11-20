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
use Keboola\DbWriter\Snowflake\Test\S3Loader;
use Keboola\StorageApi\Client;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

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
        $this->storageApi = new Client([
            'token' => getenv('KBC_TOKEN'),
            'url' => getenv('KBC_URL'),
        ]);

        // cleanup KBC storage
        $bucketId = 'in.c-test-wr-db-snowflake';
        if ($this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->dropBucket($bucketId, ['force' => true]);
        }

        $this->storageApi->createBucket('test-wr-db-snowflake', 'in');

        $this->tmpRunDir = '/tmp/' . uniqid('wr-db-snowflake_');
        mkdir($this->tmpRunDir . '/in/tables/', 0777, true);
        $config = $this->initConfig();

        foreach ($config['parameters']['tables'] as $table) {
            $this->storageApi->createTableAsync(
                $bucketId,
                $table['tableId'],
                new CsvFile($this->dataDir . '/in/tables/' . $table['tableId'] . '.csv')
            );
        }
    }

    public function testRun()
    {
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpRunDir . ' 2>&1', null, null, null, 180);
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), 'Output: ' . $process->getOutput());
    }

    public function testRunAllIgnored()
    {
        $config = $this->initConfig(function ($config) {
            $tables = array_map(function ($table) {
                $table['items'] = array_map(function ($item) {
                    $item['type'] = 'IGNORE';
                    return $item;
                }, $table['items']);
                return $table;
            }, $config['parameters']['tables']);
            $config['parameters']['tables'] = $tables;

            return $config;
        });

        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpRunDir, null, null, null, 180);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
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
        $yaml = new Yaml();
        $dstConfigPath = $this->tmpRunDir . '/config.yml';
        $config = $yaml->parse(file_get_contents($this->dataDir . '/config.yml'));

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

        $config['storage'] = ['input' => ['tables' => []]];
        foreach ($config['parameters']['tables'] as $key => $table)
        {
            $config['storage']['input']['tables'][] = [
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
        }

        @unlink($dstConfigPath);
        file_put_contents($dstConfigPath, $yaml->dump($config));

        return $config;
    }
}
