<?php
namespace Keboola\DbWriter\Snowflake\Application;

use Keboola\DbWriter\Configuration\Validator;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\StorageApi\Client;
use Symfony\Component\Config\Definition\ConfigurationInterface;

abstract class Base implements IApplication
{
    /**
     * @var Client
     */
    protected $sapiClient;

    /**
     * @var Logger
     */
    protected $logger;

    /**
     * @var \Closure
     */
    protected $validator;

    public function __construct(Client $sapiClient, Logger $logger, ConfigurationInterface $configDefinition)
    {
        $this->sapiClient = $sapiClient;
        $this->logger = $logger;
        $this->validator = Validator::getValidator($configDefinition);
    }

    public function run($action, array $config)
    {
        //@FIXME nezapomenout na run id?
        //@FIXME validate client ?
        $params = call_user_func_array($this->validator, [$config['parameters']]);

        $actionMethod = $action . 'Action';
        if (!method_exists($this, $actionMethod)) {
            throw new UserException(sprintf("Action '%s' does not exist.", $action));
        } else {
            $this->logger->info(sprintf("Executing '%s' action.", $action));
        }

        return $this->$actionMethod($params);
    }

    abstract protected function testConnectionAction(array $config);

    abstract protected function testWorkspaceAction(array $config);

    abstract protected function runAction(array $config);
}