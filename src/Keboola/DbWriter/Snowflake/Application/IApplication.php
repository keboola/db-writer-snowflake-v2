<?php
namespace Keboola\DbWriter\Snowflake\Application;

interface IApplication
{
    public function run($action, array $config);
}
