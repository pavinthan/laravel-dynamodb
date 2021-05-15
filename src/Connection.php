<?php

namespace BrainLK\DynamoDb;

use Illuminate\Database\Connection as BaseConnection;
use InvalidArgumentException;
use Aws\DynamoDb\DynamoDbClient;

class Connection extends BaseConnection
{
    /**
     * The DynamoDb connection handler.
     * @var \Aws\DynamoDb\DynamoDbClient
     */
    protected DynamoDbClient $connection;

    /**
     * The DynamoDb database handler.
     * @var string
     */
    protected string $database;

    /**
     * Create a new database connection instance.
     * @param array $config
     */
    public function __construct(array $config)
    {
        $this->config = $config;

        // Create the connection
        $this->connection = $this->createConnection($config);

        // Select database
        $this->database = $this->getDefaultDatabaseName($config);

        $this->useDefaultPostProcessor();

        $this->useDefaultSchemaGrammar();

        $this->useDefaultQueryGrammar();
    }

    /**
     * Begin a fluent query against a database collection.
     * @param string $collection
     * @return Query\Builder
     */
    public function collection($collection)
    {
        $query = new Query\Builder($this, $this->getPostProcessor());

        return $query->from($collection);
    }

    /**
     * Begin a fluent query against a database collection.
     * @param string $table
     * @param string|null $as
     * @return Query\Builder
     */
    public function table($table, $as = null)
    {
        return $this->collection($table);
    }

    /**
     * @inheritdoc
     */
    public function getSchemaBuilder()
    {
        return new Schema\Builder($this);
    }

    /**
     * return DynamoDb object.
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    public function getDynamoDbClient()
    {
        return $this->connection;
    }

    /**
     * {@inheritdoc}
     */
    public function getDatabaseName()
    {
        return $this->database;
    }

    /**
     * Get the name of the default database based on db config or try to detect it from dsn
     * @param array $config
     * @return string
     * @throws InvalidArgumentException
     */
    protected function getDefaultDatabaseName($config)
    {
        return $config['database'];
    }

    /**
     * Create a new DynamoDbClient connection.
     * @param array $config
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    protected function createConnection(array $config)
    {
        return new DynamoDbClient($config);
    }

    /**
     * @inheritdoc
     */
    public function disconnect()
    {
        unset($this->connection);
    }

    /**
     * @inheritdoc
     */
    public function getElapsedTime($start)
    {
        return parent::getElapsedTime($start);
    }

    /**
     * @inheritdoc
     */
    public function getDriverName()
    {
        return 'dynamodb';
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultPostProcessor()
    {
        return new Query\Processor();
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultQueryGrammar()
    {
        return new Query\Grammar();
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultSchemaGrammar()
    {
        return new Schema\Grammar();
    }

    /**
     * Set database.
     * @param string $db
     */
    public function setDatabase(string $database)
    {
        $this->database = $database;
    }

    /**
     * Dynamically pass methods to the connection.
     * @param string $method
     * @param array $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return call_user_func_array([$this->connection, $method], $parameters);
    }
}
