<?php
namespace Keboola\DbWriter\Snowflake\Application;

use Keboola\DbWriter\Snowflake\Configuration\WorkspaceDefinition;
use Keboola\DbWriter\Snowflake\Exception\UserException;
use Keboola\DbWriter\Snowflake\Logger\Logger;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;

class Workspace extends Base
{
    public function __construct(Client $sapiClient, Logger $logger)
    {
        parent::__construct($sapiClient, $logger, new WorkspaceDefinition());
    }

    protected function testConnectionAction(array $params, array $mapping)
    {
        $workspaces = new Workspaces($this->sapiClient);
        // @TODO validate if workspace is assigned to our compoennt
        try {
            $workspaces->getWorkspace($params['workspaceId']);
        } catch (ClientException $e) {
            throw new UserException($e->getMessage());
        }

        return [
            'status' => 'success',
        ];
    }

    protected function runAction(array $params, array $mapping)
    {
        $uploaded = [];

        $workspaces = new Workspaces($this->sapiClient);
        $tables = $this->filterTables($params['tables']);

        $whereValues = [];
        if (isset($mapping['input']['tables'])) {
            foreach ($mapping['input']['tables'] as $mappingTable) {
                $whereValues[$mappingTable['source']] = [
                    'whereColumn' => $mappingTable['where_column'],
                    'whereValues' => $mappingTable['where_values'],
                    'whereOperator' => 'eq',
                ];
            }
        }

        foreach ($tables as $table) {
            //@TODO use only one workspace load
            $this->logger->info(sprintf('Trying load table: "%s"', $table['tableId']));
            $options = [
                'input' => [
                    [
                        'source' => $table['tableId'],
                        'destination' => $table['dbName'],
                        'incremental' => $table['incremental'],
                        'columns' => [],
                    ],
                ],
            ];

            // columns specification
            foreach ($table['items'] as $item) {
                $options['input'][0]['columns'][] = [
                    'source' => $item['name'],
                    'destination' => $item['dbName'],
                    'type' => $item['type'],
                    'length' => $item['size'],
                    'nullable' => $item['nullable'],
                    'convertEmptyValuesToNull' => $item['nullable'],
                ];
            }

            // where filters
            if (isset($whereValues[$table['tableId']])) {
                $options['input'][0]['whereColumn'] = $whereValues[$table['tableId']]['whereColumn'];
                $options['input'][0]['whereValues'] = $whereValues[$table['tableId']]['whereValues'];
                $options['input'][0]['whereOperator'] = $whereValues[$table['tableId']]['whereOperator'];
            }

            try {
                // @TODO validate if workspace is assigned to our compoennt
                $workspaces->loadWorkspaceData($params['workspaceId'], $options);
            } catch (ClientException $e) {
                throw new UserException($e->getMessage());
            }

            $uploaded[] = $table['tableId'];
        }

        return [
            'status' => 'success',
            'uploaded' => $uploaded
        ];
    }
}
