<?php

namespace Merkeleon\Nsq\Queue;


use Illuminate\Queue\Worker as BaseWorker;
use Illuminate\Queue\WorkerOptions;

class Worker extends BaseWorker
{
    protected function registerTimeoutHandler($job, WorkerOptions $options)
    {
        return false;
    }
}
