<?php
namespace Keboola\DbWriter\Snowflake\Application;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Snowflake\Logger\Logger;
use Keboola\StorageApi\Client;

abstract class BaseTest extends \PHPUnit_Framework_TestCase
{
    const CONFIGURATION_DESCRIPTION = 'Keboola DbWriter Snowflake 2 TEST';

    protected $dataDir = ROOT_PATH . "/tests/data";

    /** @var Client */
    protected $storageApi;

    /**
     * @var Logger
     */
    protected $logger;

    public function setUp()
    {
        $this->storageApi = new Client([
            'token' => getenv('KBC_TOKEN'),
            'url' => getenv('KBC_URL'),
        ]);

        $this->logger = new Logger('tests', true);
    }

    protected function prepareSapiTables()
    {
        // cleanup KBC storage
        $bucketId = 'in.c-test-wr-db-snowflake';
        if ($this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->dropBucket($bucketId, ['force' => true]);
        }

        $this->storageApi->createBucket('test-wr-db-snowflake', 'in');

        $tableId = $this->storageApi->createTableAsync(
            $bucketId,
            'simple',
            new CsvFile($this->dataDir . '/incremental/in/tables/simple.csv')
        );

        $this->storageApi->createTablePrimaryKey($tableId, ['id']);
        $this->storageApi->markTableColumnAsIndexed($tableId, 'glasses');
    }

    abstract public function testConnect();
}
