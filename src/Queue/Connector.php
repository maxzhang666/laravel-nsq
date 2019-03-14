<?php

namespace Merkeleon\Nsq\Queue;


use Illuminate\Queue\Connectors\ConnectorInterface;


class Connector implements ConnectorInterface
{
    /**
     * Establish a queue connection.
     * @param array $config
     * @return \Illuminate\Contracts\Queue\Queue
     * @throws \Exception
     */
    public function connect(array $config)
    {
        return new NsqQueue($config);
    }
}
