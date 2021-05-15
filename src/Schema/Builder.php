<?php

namespace BrainLK\DynamoDb\Schema;

use Closure;
use Illuminate\Database\Schema\Builder as BaseBuilder;

class Builder extends BaseBuilder
{
    /**
     * @inheritdoc
     */
    public function hasColumn($table, $column)
    {
        return true;
    }

    /**
     * @inheritdoc
     */
    public function hasColumns($table, array $columns)
    {
        return true;
    }

    /**
     * @inheritdoc
     */
    public function hasTable($collection)
    {
        return true;
    }

    /**
     * Modify a collection on the schema.
     * @param string $collection
     * @param Closure $callback
     * @return bool
     */
    public function collection($collection, Closure $callback)
    {
        $blueprint = $this->createBlueprint($collection);

        if ($callback) {
            $callback($blueprint);
        }
    }

    /**
     * @inheritdoc
     */
    public function table($collection, Closure $callback)
    {
        return $this->collection($collection, $callback);
    }

    /**
     * @inheritdoc
     */
    public function drop($collection)
    {
        $blueprint = $this->createBlueprint($collection);

        return $blueprint->drop();
    }

    /**
     * @inheritdoc
     */
    protected function createBlueprint($collection, Closure $callback = null)
    {
        return new Blueprint($this->connection, $collection);
    }
}
