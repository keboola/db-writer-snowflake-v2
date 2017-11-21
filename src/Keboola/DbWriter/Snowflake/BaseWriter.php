<?php
/**
 * Created by Miroslav Čillík <miro@keboola.com>
 * Date: 05/09/14
 * Time: 12:53
 */

namespace Keboola\DbWriter\Snowflake;

use Keboola\DbWriter\Snowflake\Exception\ApplicationException;
use Keboola\DbWriter\Snowflake\Exception\UserException;
use Keboola\DbWriter\Snowflake\Logger\Logger;
use Keboola\SSHTunnel\SSH;
use Keboola\SSHTunnel\SSHException;

abstract class BaseWriter
{
    protected $db;

    protected $async = false;

    /** @var Logger */
    protected $logger;

    protected $dbParams;

    public function __construct($dbParams, Logger $logger)
    {
        $this->logger = $logger;

        if (isset($dbParams['ssh']['enabled']) && $dbParams['ssh']['enabled']) {
            $dbParams = $this->createSshTunnel($dbParams);
        }
        $this->dbParams = $dbParams;

        try {
            $this->db = $this->createConnection($this->dbParams);
        } catch (\Exception $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new ApplicationException("Missing driver: " . $e->getMessage());
            }
            throw new UserException("Error connecting to DB: " . $e->getMessage(), 0, $e);
        }
    }

    public function createSshTunnel($dbConfig)
    {
        $sshConfig = $dbConfig['ssh'];

        // check params
        foreach (['keys', 'sshHost'] as $k) {
            if (empty($sshConfig[$k])) {
                throw new UserException(sprintf("Parameter %s is missing.", $k));
            }
        }

        if (empty($sshConfig['user'])) {
            $sshConfig['user'] = $dbConfig['user'];
        }
        if (empty($sshConfig['localPort'])) {
            $sshConfig['localPort'] = 33006;
        }
        if (empty($sshConfig['remoteHost'])) {
            $sshConfig['remoteHost'] = $dbConfig['host'];
        }
        if (empty($sshConfig['remotePort'])) {
            $sshConfig['remotePort'] = $dbConfig['port'];
        }
        if (empty($sshConfig['sshPort'])) {
            $sshConfig['sshPort'] = 22;
        }

        $sshConfig['privateKey'] = empty($sshConfig['keys']['#private'])
            ?$sshConfig['keys']['private']
            :$sshConfig['keys']['#private'];

        $tunnelParams = array_intersect_key($sshConfig, array_flip([
            'user', 'sshHost', 'sshPort', 'localPort', 'remoteHost', 'remotePort', 'privateKey'
        ]));

        $this->logger->info("Creating SSH tunnel to '" . $tunnelParams['sshHost'] . "'");

        try {
            $ssh = new SSH();
            $ssh->openTunnel($tunnelParams);
        } catch (SSHException $e) {
            throw new UserException($e->getMessage());
        }

        $dbConfig['host'] = '127.0.0.1';
        $dbConfig['port'] = $sshConfig['localPort'];

        return $dbConfig;
    }

    public function getConnection()
    {
        return $this->db;
    }
}
