<?php
/**
 * Application endpoint
 *
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
use Keboola\DbWriter\Snowflake\Exception\ApplicationException;
use Keboola\DbWriter\Snowflake\Exception\UserException;
use Keboola\DbWriter\Snowflake\Logger\Logger;
use Keboola\DbWriter\Snowflake\Application;
use Monolog\Handler\NullHandler;
use Symfony\Component\Yaml\Yaml;

define('APP_NAME', 'wr-db-snowflake-v2');
define('ROOT_PATH', __DIR__);

require_once(dirname(__FILE__) . "/bootstrap.php");

$logger = new Logger(APP_NAME);

$action = 'run';

try {
    if (!getenv('KBC_TOKEN')) {
        throw new UserException("Missing KBC API token");
    }
    if (!getenv('KBC_URL')) {
        throw new UserException("Missing KBC API url");
    }

    // setup SAPI
    $client = new \Keboola\StorageApi\Client([
        'token' => getenv('KBC_TOKEN'),
        'url' => getenv('KBC_URL'),
    ]);

    if (getenv('KBC_RUNID')) {
        $client->setRunId(getenv('KBC_RUNID'));
    }

    // configuration load
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    $configFile = new SplFileInfo($arguments["data"] . "/config.json");
    if (!$configFile->isFile()) {
        throw new UserException('Missing configuration file in data folder.');
    }

    $config = json_decode(file_get_contents($configFile), true);
    $config['parameters']['data_dir'] = $arguments['data'];
    $action = isset($config['action']) ? $config['action'] : $action;

    // app init
    if (isset($config['parameters']['workspaceId'])) {
        $app = new Application\Workspace($client, $logger);
    } elseif (isset($config['parameters']['db'])) {
        $app = new Application\Connect($client, $logger);
    } else {
        throw new UserException("Unsupported configuration. Missing 'workspaceId' or 'db' parameters");
    }

    if ($action !== 'run') {
        $logger->setHandlers(array(new NullHandler(Logger::INFO)));
    }

    echo json_encode($app->run($action, $config));
} catch (UserException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());

    if ($action !== 'run') {
        echo $e->getMessage();
    }

    exit(1);
} catch (ApplicationException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    exit($e->getCode() > 1 ? $e->getCode(): 2);
} catch (\Exception $e) {
    $logger->log('error', $e->getMessage(), [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
    //    'trace' => $e->getTrace()
    ]);
    exit(2);
}
exit(0);
