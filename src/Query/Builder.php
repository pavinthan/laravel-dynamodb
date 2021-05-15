<?php

namespace BrainLK\DynamoDb\Query;

use BrainLK\DynamoDb\Connection;
use Illuminate\Database\Query\Builder as BaseBuilder;

class Builder extends BaseBuilder
{
    /**
     * @inheritdoc
     */
    public function __construct(Connection $connection, Processor $processor)
    {
        $this->grammar = new Grammar;
        $this->connection = $connection;
        $this->processor = $processor;
    }
}
