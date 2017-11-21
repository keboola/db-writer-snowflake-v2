<?php
namespace Keboola\DbWriter\Snowflake\Application;

use Keboola\DbWriter\Snowflake\Configuration\StorageDefinition;
use Keboola\DbWriter\Snowflake\Exception\UserException;
use Keboola\DbWriter\Snowflake\Logger\Logger;
use Keboola\StorageApi\Client;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Exception\Exception as ConfigException;
use Symfony\Component\Config\Definition\Processor;

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
    protected $paramsValidator;

    /**
     * @var \Closure
     */
    protected $mappingValidator;

    public function __construct(Client $sapiClient, Logger $logger, ConfigurationInterface $configDefinition)
    {
        $this->sapiClient = $sapiClient;
        $this->logger = $logger;
        $this->paramsValidator = $this->createValidator($configDefinition);
        $this->mappingValidator = $this->createValidator(new StorageDefinition());
    }

    protected function createValidator(ConfigurationInterface $definition)
    {
        return function ($parameters) use ($definition) {
            try {
                $processor = new Processor();
                $processedParameters = $processor->processConfiguration(
                    $definition,
                    [$parameters]
                );

                if (!empty($processedParameters['db']['#password'])) {
                    $processedParameters['db']['password'] = $processedParameters['db']['#password'];
                }

                return $processedParameters;
            } catch (ConfigException $e) {
                throw new UserException($e->getMessage(), 0, $e);
            }
        };
    }

    public function run($action, array $config)
    {
        if (!isset($config['parameters'])) {
            $config['parameters'] = [];
        }

        if (!isset($config['storage'])) {
            $config['storage'] = [];
        }
        //@FIXME nezapomenout na run id?
        //@FIXME validate client ?
        $params = call_user_func_array($this->paramsValidator, [$config['parameters']]);
        $mapping = call_user_func_array($this->mappingValidator, [$config['storage']]);

        $actionMethod = $action . 'Action';
        if (!method_exists($this, $actionMethod)) {
            throw new UserException(sprintf("Action '%s' does not exist.", $action));
        } else {
            $this->logger->info(sprintf("Executing '%s' action.", $action));
        }

        return $this->$actionMethod($params, $mapping);
    }

    abstract protected function testConnectionAction(array $config, array $mapping);

    abstract protected function runAction(array $config, array $mapping);
}
