<?php

namespace BrainLK\DynamoDb\Queue\Failed;

use Carbon\Carbon;
use Illuminate\Queue\Failed\DatabaseFailedJobProvider;

class DynamoDbFailedJobProvider extends DatabaseFailedJobProvider
{
    /**
     * Log a failed job into storage.
     * @param string $connection
     * @param string $queue
     * @param string $payload
     * @param  \Exception  $exception
     * @return void
     */
    public function log($connection, $queue, $payload, $exception)
    {
        $failed_at = Carbon::now()->getTimestamp();

        $exception = (string) $exception;

        $this->getTable()->insert(compact('connection', 'queue', 'payload', 'failed_at', 'exception'));
    }

    /**
     * Get a list of all of the failed jobs.
     * @return object[]
     */
    public function all()
    {
        $all = $this->getTable()->orderBy('id', 'desc')->get()->all();

        return array_map(function ($job) {
            return (object) $job;
        }, $all);
    }

    /**
     * Get a single failed job.
     * @param mixed $id
     * @return object
     */
    public function find($id)
    {
        $job = $this->getTable()->find($id);

        if (!$job) {
            return;
        }

        return (object) $job;
    }

    /**
     * Delete a single failed job from storage.
     * @param mixed $id
     * @return bool
     */
    public function forget($id)
    {
        return $this->getTable()->where('id', $id)->delete() > 0;
    }
}
