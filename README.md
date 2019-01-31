# merkeleon-nsq
Laravel package for NSQ queue manager that uses Laravel's API

## Installation
First, require the package using Composer:

`composer require merkeleon/laravel-nsq`

1. Add new queue job as described in Laravel's manual:

`php artisan make:job <JobName>` Edit this file according to Laravel's rules

2. Set queue driver to NSQ

`QUEUE_DRIVER=nsq`

3. Set env options for NSQ servers:

`NSQSD_URL=127.0.0.1:4150` IP and port for Nsq daemon

`NSQLOOKUP_URL=127.0.0.1:4161` IP and port for Nsq lookup daemon

Use comma as separator if you want to use several servers:

`NSQSD_URL=127.0.0.1:4150,127.0.0.1:4151,127.0.0.1:4152`


## Example
### Job class
```
<?php

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Queue\SerializesModels;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Support\Facades\Config;

class CoolJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public function __construct(...$args)
    {
        $this->queue = 'my-cool-jobs';
        
        // $args code
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        // do the job
    }
}
```

### Some where in a code
```
<?php

... code ...

// Push timed task in the queue
CoolJob::dispatch(...$any_args);

... code ...
```
