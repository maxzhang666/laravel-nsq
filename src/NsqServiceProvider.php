<?php

namespace Merkeleon\Nsq;


use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Queue\QueueServiceProvider;
use Merkeleon\Nsq\Queue\Connector as NsqConnector;
use Merkeleon\Nsq\Queue\Worker;

class NsqServiceProvider extends QueueServiceProvider
{
    /**
     * Register the connectors on the queue manager.
     *
     * @param  \Illuminate\Queue\QueueManager  $manager
     * @return void
     */
    public function registerConnectors($manager)
    {
        parent::registerConnectors($manager);

        $this->registerNsqConnector($manager);
    }

    /**
     * Register the connectors on the queue manager.
     *
     * @param  \Illuminate\Queue\QueueManager  $manager
     * @return \Illuminate\Queue\Connectors\ConnectorInterface
     */
    public function registerNsqConnector($manager)
    {
        $manager->addConnector('nsq', function () {
            return new NsqConnector($this->app['nsq']);
        });
    }

    /**
     * Register the queue worker.
     *
     * @return void
     */
    protected function registerWorker()
    {
        $this->app->singleton('queue.worker', function () {
            return new Worker(
                $this->app['queue'], $this->app['events'], $this->app[ExceptionHandler::class]
            );
        });
    }
}