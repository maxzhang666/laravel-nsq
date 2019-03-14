<?php

namespace Merkeleon\Nsq\Tunnel;


use OkStuff\PhpNsq\Tunnel\Config;

class Pool
{
    private $pool = [];

    public function __construct($nsq)
    {
        foreach ($nsq['nsq']['nsqd-addrs'] as $value)
        {
            $addr = explode(':', $value);
            $this->addTunnel(new Tunnel(
                new Config($addr[0], $addr[1])
            ));

            $tunnel = $this->getTunnel();
            $tunnel->setIdentify($nsq['identify']);
        }
    }

    public function addTunnel(Tunnel $tunnel)
    {
        $this->pool[] = $tunnel;

        return $this;
    }

    /**
     * @return Tunnel
     */
    public function getTunnel()
    {
        return $this->pool[array_rand($this->pool)];
    }
}