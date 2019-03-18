<?php

return [
    'driver' => 'nsq',
    'channel' => 'web',
    "nsq"    => [
        "nsqd-addrs" => array_filter(explode(',', env('NSQSD_URL', '127.0.0.1:9150'))),
        "logdir"     => "/tmp",
    ],
    'identify' => [
        'user_agent' => 'merkeleon/laravel-nsq-1.10',
    ],
];
