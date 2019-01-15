<?php

namespace Merkeleon\Nsq\Queue;


use Illuminate\Queue\Console\WorkCommand as BaseWorkCommand;
use Illuminate\Support\Facades\Config;

class WorkCommand extends BaseWorkCommand
{
    /**
     * Execute the console command.
     *
     * @return void
     */
    public function handle()
    {
        $this->hasOption('queue') && Config::set(['consumer' => 1]);

        if (method_exists(get_parent_class($this), 'handle'))
        {
            parent::handle();
        }

        parent::fire();
    }
}