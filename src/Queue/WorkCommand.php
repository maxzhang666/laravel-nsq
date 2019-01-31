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
        Config::set(['consumer' => 1]);

        parent::handle();
    }
}