<?php

return [
    'driver'    => 'nsq',
    'topic'     => 'default', // Default topic to push to
    'channel'   => 'web',
    'nsq'       => [
        'addresses' => array_filter(explode(',', env('NSQSD_URL', '127.0.0.1:9150'))),
        'logdir'    => '/tmp',
    ],
    'nsqlookup' => [
        'addresses' => array_filter(explode(',', env('NSQLOOKUP_URL', '127.0.0.1:9150'))),
    ],
    'identify'  => [
        'user_agent' => 'merkeleon/laravel-nsq-1.13',
    ],
];
