<?php
namespace Keboola\DbWriter\Snowflake\Test;

use Keboola\DbWriter\Snowflake\Logger\Logger;
use Keboola\DbWriter\Snowflake\Writer;

class BaseTest extends \PHPUnit_Framework_TestCase
{
    protected $dataDir = ROOT_PATH . "/tests/data";

    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-common');
        }
    }

    protected function getConfig($driver)
    {
        $config = json_decode(file_get_contents($this->dataDir . '/' .$driver . '/config.json'), true);

        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv($driver, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv($driver, 'DB_SCHEMA');
        $config['parameters']['db']['warehouse'] = $this->getEnv($driver, 'DB_WAREHOUSE');
        $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];

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

    protected function getWriter($parameters)
    {
        return new Writer($parameters['db'], new Logger(APP_NAME));
    }
}
