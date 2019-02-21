<?php

namespace Merkeleon\Nsq\Monitor;


use Illuminate\Support\Arr;
use Jiyis\Nsq\Monitor\Consumer as NsqConsumer;

class Consumer extends NsqConsumer
{
    public function __construct($host, array $config, $topic, $channel)
    {
        $identify = Arr::get($this->config, 'identify');
        Arr::set($identify, 'client_id', 'client-' . $topic);
        Arr::set($this->config, 'identify', $identify);

        parent::__construct($host, $config, $topic, $channel);
    }
}