<?php

namespace Merkeleon\Nsq;


use Merkeleon\Nsq\Providers\WorkCommandProvider;
use Merkeleon\Nsq\Queue\Connector;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;

class NsqServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
    }

    /**
     * Register the application's event listeners.
     *
     * @return void
     */
    public function boot()
    {
        /** @var QueueManager $queue */
        $queue = $this->app['queue'];

        $queue->addConnector('nsq', function () {
            return new Connector();
        });
        // add defer provider, rebind work command
        $this->app->addDeferredServices([WorkCommandProvider::class]);
    }
}