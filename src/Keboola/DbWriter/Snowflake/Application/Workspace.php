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
        // @FIXME validate if workspace is assigned to our compoennt
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
        $workspaces = new Workspaces($this->sapiClient);

        $uploaded = [];
        $tables = array_filter($params['tables'], function ($table) {
            return ($table['export']);
        });

        foreach ($tables as $table) {
            $this->logger->info(sprintf('Trying load table: "%s"', $table['tableId']));
            $options = [
                'input' => [
                    [
                        'source' => $table['tableId'],
                        'destination' => $table['dbName'],
                        'incremental' => $table['incremental'],
                        'datatypes' => [],
                        'columns' => [],
                    ],
                ],
            ];


            foreach ($table['items'] as $item) {
                $options['input'][0]['columns'][] = $item['name'];

                $options['input'][0]['datatypes'][$item['name']] = [
                    'column' => $item['name'],
                    'type' => $item['type'],
                    'length' => $item['size'],
                    'nullable' => $item['nullable'],
                    'convertEmptyValuesToNull' => $item['nullable'],
                ];
            }

            try {
                $hovno = $workspaces->loadWorkspaceData($params['workspaceId'], $options);
                var_dump($hovno);
            } catch (ClientException $e) {
                //@TODO error code better conversion
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