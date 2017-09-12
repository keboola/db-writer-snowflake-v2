<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 28/04/16
 * Time: 09:31
 */

namespace Keboola\DbWriter\Snowflake;

use Keboola\DbWriter\Snowflake\Exception\UserException;

class Connection
{
    /**
     * @var resource odbc handle
     */
    private $connection;

    /**
     * The connection constructor accepts the following options:
     * - host (string, required) - hostname
     * - port (int, optional) - port - default 443
     * - user (string, required) - username
     * - password (string, required) - password
     * - warehouse (string) - default warehouse to use
     * - database (string) - default database to use
     * - tracing (int) - the level of detail to be logged in the driver trace files
     * - loginTimeout (int) - Specifies how long to wait for a response when connecting to the Snowflake service before returning a login failure error.
     * - networkTimeout (int) - Specifies how long to wait for a response when interacting with the Snowflake service before returning an error. Zero (0) indicates no network timeout is set.
     * - queryTimeout (int) - Specifies how long to wait for a query to complete before returning an error. Zero (0) indicates to wait indefinitely.
     *
     * @param array $options
     * @throws UserException
     */
    public function __construct(array $options)
    {
        $requiredOptions = [
            'host',
            'user',
            'password',
            'database',
            'schema'
        ];

        $missingOptions = array_diff($requiredOptions, array_keys($options));
        if (!empty($missingOptions)) {
            throw new UserException('Missing options: ' . implode(', ', $missingOptions));
        }

        $port = isset($options['port']) ? (int) $options['port'] : 443;
        $tracing = isset($options['tracing']) ? (int) $options['tracing'] : 0;
        $maxBackoffAttempts = isset($options['maxBackoffAttempts']) ? (int) $options['maxBackoffAttempts'] : 5;
        $loginTimeout = isset($options['loginTimeout']) ? (int) $options['loginTimeout'] : 30;

        $dsn = "Driver=SnowflakeDSIIDriver;Server=" . $options['host'];
        $dsn .= ";Port=" . $port;
        $dsn .= ";Tracing=" . $tracing;
        $dsn .= ";Login_timeout=" . $loginTimeout;
        $dsn .= ";Database=" . $this->quoteIdentifier($options['database']);
        $dsn .= ";Schema=" . $this->quoteIdentifier($options['schema']);

        if (isset($options['networkTimeout'])) {
            $dsn .= ";Network_timeout=" . (int) $options['networkTimeout'];
        }

        if (isset($options['queryTimeout'])) {
            $dsn .= ";Query_timeout=" . (int) $options['queryTimeout'];
        }

        if (isset($options['warehouse'])) {
            $dsn .= ";Warehouse=" . $this->quoteIdentifier($options['warehouse']);
        }

        $attemptNumber = 0;
        do {
            if ($attemptNumber > 0) {
                sleep(pow(2, $attemptNumber));
            }
            try {
                $this->connection = odbc_connect($dsn, $options['user'], $options['password']);
            } catch (\Exception $e) {
                // try again if it is a failed rest request
                if (stristr($e->getMessage(), "S1000") !== false) {
                    $attemptNumber++;
                    if ($attemptNumber > $maxBackoffAttempts) {
                        throw new UserException("Initializing Snowflake connection failed: " . $e->getMessage(), null, $e);
                    }
                } else {
                    throw new UserException("Initializing Snowflake connection failed: " . $e->getMessage(), null, $e);
                }
            }
        } while ($this->connection === null);
    }

    public function quoteIdentifier($value)
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    /**
     * Returns information about table:
     *  - name
     *  - bytes
     *  - rows
     * @param $schemaName
     * @param $tableName
     * @return array
     * @throws UserException
     */
    public function describeTable($schemaName, $tableName)
    {
        $tables = $this->fetchAll(sprintf(
            "SHOW TABLES LIKE %s IN SCHEMA %s",
            "'" . addslashes($tableName) . "'",
            $this->quoteIdentifier($schemaName)
        ));

        foreach ($tables as $table) {
            if ($table['name'] === $tableName) {
                return $table;
            }
        }

        throw new UserException("Table $tableName not found in schema $schemaName");
    }

    public function describeTableColumns($schemaName, $tableName)
    {
        return $this->fetchAll(sprintf(
            'SHOW COLUMNS IN %s.%s',
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName)
        ));
    }

    public function getTableColumns($schemaName, $tableName)
    {
        return array_map(function ($column) {
            return $column['column_name'];
        }, $this->describeTableColumns($schemaName, $tableName));
    }

    public function getTablePrimaryKey($schemaName, $tableName)
    {
        $cols = $this->fetchAll(sprintf(
            "DESC TABLE %s.%s",
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName)
        ));
        $pkCols = [];
        foreach ($cols as $col) {
            if ($col['primary key'] !== 'Y') {
                continue;
            }
            $pkCols[] = $col['name'];
        }

        return $pkCols;
    }

    public function query($sql, array $bind = [])
    {
        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt, $this->repairBinding($bind));
        odbc_free_result($stmt);
    }

    public function fetchAll($sql, $bind = [])
    {
        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt, $this->repairBinding($bind));
        $rows = [];
        while ($row = odbc_fetch_array($stmt)) {
            $rows[] = $row;
        }
        odbc_free_result($stmt);
        return $rows;
    }

    public function fetch($sql, $bind, callable $callback)
    {
        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt, $this->repairBinding($bind));
        while ($row = odbc_fetch_array($stmt)) {
            $callback($row);
        }
        odbc_free_result($stmt);
    }

    /**
     * Avoid odbc file open http://php.net/manual/en/function.odbc-execute.php
     * @param array $bind
     * @return array
     */
    private function repairBinding(array $bind)
    {
        return array_map(function ($value) {
            if (preg_match("/^'.*'$/", $value)) {
                return " {$value} ";
            } else {
                return $value;
            }
        }, $bind);
    }
}
