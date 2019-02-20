<?php

namespace Merkeleon\Nsq\Queue;


use Illuminate\Support\Facades\Config;
use Illuminate\Support\Arr;
use Jiyis\Nsq\Adapter\NsqClientManager as JiyisNsqClientManager;
use Jiyis\Nsq\Lookup\Lookup;
use Jiyis\Nsq\Monitor\Consumer;
use Jiyis\Nsq\Monitor\Producer;

class ClientManager extends JiyisNsqClientManager
{
    public function connect()
    {
        $this->connectTime = time();
        /**
         * if topic and channel is not null, then the command is sub
         */
        if (Config::get('consumer') && !empty($this->topic))
        {
            $this->consumerPool = $this->setUpConsumers();
        }
        else
        {
            $this->producerPool = $this->setUpProducers();
        }
    }

    /**
     * reflect job, get topic and channel
     * @throws \ReflectionException
     */
    public function reflectionJob()
    {
    }

    public function setUpProducers()
    {
        /**
         * if topic and channel is null, then the command is pub
         */
        $hosts = Arr::get($this->config, 'connection.nsqd_url', ['127.0.0.1:4150']);
        $pool  = [];

        foreach ($hosts as $item)
        {
            $pool[$item] = new Producer($item, $this->config);
        }

        return $pool;
    }

    public function setUpConsumers()
    {
        $channel  = Arr::get($this->config, 'channel', 'web');
        $lookup   = new Lookup(Arr::get($this->config, 'connection.nsqlookup_url', ['127.0.0.1:4161']));
        $nsqdList = $lookup->lookupHosts($this->topic);
        $pool     = [];

        foreach ($nsqdList['lookupHosts'] as $item)
        {
            $pool[$item] = new Consumer($item, $this->config, $this->topic, $channel);
        }

        return $pool;
    }

    public function setTopic($topic): self
    {
        $this->topic = $topic;

        return $this;
    }

    public function setChannel($channel = null): self
    {
        if (empty($channel))
        {
            $channel = Arr::get($this->config, 'channel', 'default');
        }
        $this->channel = $channel;

        return $this;
    }
}