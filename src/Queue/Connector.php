<?php

namespace Merkeleon\Nsq\Queue;


use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Support\Arr;

class Connector implements ConnectorInterface
{
    /**
     * Establish a queue connection.
     * @param array $config
     * @return \Illuminate\Contracts\Queue\Queue|NsqQueue
     * @throws \Exception
     */
    public function connect(array $config)
    {
        $client = new ClientManager($config);

        return new Queue(
            $client,
            Arr::get($config, 'retry_delay_time', 60)
        );
    }
}
