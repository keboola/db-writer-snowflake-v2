<?php

namespace Keboola\DbWriter\Snowflake\StorageApi;

use Keboola\DbWriter\Snowflake\Exception\UserException;
use Keboola\DbWriter\Snowflake\Logger\Logger;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Reader;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;

class DataLoader
{
    /**
     * @var Client
     */
    private $sapiClient;

    /**
     * @var Logger
     */
    private $logger;

    /**
     * @var string
     */
    private $dataDirectory;

    /**
     * @var array
     */
    private $storageConfig;

    /**
     * @var array
     */
    private $componentData = [];

    /**
     * DataLoader constructor.
     *
     * @param Client $storageClient
     * @param Logger $logger
     * @param string $dataDirectory
     * @param array $storageConfig
     * @param string $componentId
     * @throws \InvalidArgumentException
     */
    public function __construct(
        Client $storageClient,
        Logger $logger,
        $dataDirectory,
        array $storageConfig,
        $componentId
    ) {
        $this->sapiClient = $storageClient;
        $this->logger = $logger;
        $this->dataDirectory = $dataDirectory;
        $this->storageConfig = $storageConfig;

        $this->loadComponentData($componentId);
    }

    /**
     * @param $id
     * @throws \InvalidArgumentException
     */
    private function loadComponentData($id)
    {
        $components = $this->sapiClient->indexAction();
        foreach ($components["components"] as $component) {
            if ($component["id"] === $id) {
                $this->componentData = $component['data'];
                return;
            }
        }

        throw new \InvalidArgumentException("Component '{$id}' not found.");
    }

    /**
     * @throws UserException
     */
    public function loadInputData()
    {
        $reader = new Reader($this->sapiClient, $this->logger);
        $reader->setFormat($this->componentData['configuration_format']);

        try {
            if (isset($this->storageConfig['input']['tables']) && count($this->storageConfig['input']['tables'])) {
                $this->logger->debug('Downloading source tables.');
                $reader->downloadTables(
                    $this->storageConfig['input']['tables'],
                    $this->dataDirectory . DIRECTORY_SEPARATOR . 'in' . DIRECTORY_SEPARATOR . 'tables',
                    $this->getStagingStorageInput()
                );
            }
        } catch (ClientException $e) {
            throw new UserException('Cannot import data from Storage API: ' . $e->getMessage(), $e);
        } catch (InvalidInputException $e) {
            throw new UserException($e->getMessage(), $e);
        }
    }

    private function getStagingStorageInput()
    {
        if (($stagingStorage = $this->componentData['staging_storage']) !== null) {
            if (isset($stagingStorage['input'])) {
                return $stagingStorage['input'];
            }
        }
        return 'local';
    }
}