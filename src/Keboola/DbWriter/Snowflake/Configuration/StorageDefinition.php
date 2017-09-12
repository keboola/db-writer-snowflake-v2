<?php
/**
 * @author Erik Zigo <erik.zigo@keboola.com>
 */
namespace Keboola\DbWriter\Snowflake\Configuration;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Keboola\InputMapping\Configuration\Table as InputTable;

class StorageDefinition implements ConfigurationInterface
{
    /**
     * Generates the configuration tree builder.
     *
     * @return \Symfony\Component\Config\Definition\Builder\TreeBuilder The tree builder
     */
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('storage');

        $input = $rootNode
            ->children()
                ->arrayNode("input");

        $inputTable = $input
            ->children()
                ->arrayNode("tables")
                ->prototype("array")
        ;

        InputTable::configureNode($inputTable);


        return $treeBuilder;
    }
}
