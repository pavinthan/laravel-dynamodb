<?php

namespace BrainLK\DynamoDb;

use Illuminate\Queue\QueueServiceProvider;
use BrainLK\DynamoDb\Queue\Failed\DynamoDbFailedJobProvider;

class DynamoDbQueueServiceProvider extends QueueServiceProvider
{
    /**
     * @inheritdoc
     */
    protected function registerFailedJobServices()
    {
        $database = config('queue.failed.database');

        if ($this->app['db']->connection($database)->getDriverName() === 'dynamodb') {
            $this->app->singleton('queue.failer', function ($app) use ($database) {
                return new DynamoDbFailedJobProvider(
                    $app['db'],
                    $database,
                    config('queue.failed.table')
                );
            });
        } else {
            parent::registerFailedJobServices();
        }
    }
}
