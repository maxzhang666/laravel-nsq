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
            $this->setUpConsumers();
        }
        else
        {
            $this->setUpProducers();
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

        foreach ($hosts as $item)
        {
            $this->producerPool[$item] = $this->reconnectProducerClient($item);

            if (!$this->producerPool[$item])
            {
                unset($this->producerPool[$item]);
            }
        }

        if (!$this->hasConnectedProducers())
        {
            throw new \Exception('Cannot set up producer(s)');
        }

        return $this;
    }

    public function setUpConsumers()
    {
        $lookup   = new Lookup(Arr::get($this->config, 'connection.nsqlookup_url', ['127.0.0.1:4161']));
        $nsqdList = $lookup->lookupHosts($this->topic);

        foreach ($nsqdList['lookupHosts'] as $item)
        {
            $this->consumerPool[$item] = $this->reconnectConsumerClient($item);

            if (!$this->consumerPool[$item])
            {
                unset($this->consumerPool[$item]);
            }
        }

        if (!$this->hasConnectedConcumers())
        {
            throw new \Exception('Cannot set up consumer(s)');
        }

        return $this;
    }

    /**
     * @param $key
     * @throws \Exception
     */
    public function reconnectProducerClient($key)
    {
        $retry_connections = Arr::get($this->config, 'retry_num_connections', 10) ?: 1;
        $retry_connections = $retry_connections > 0 ? (int)$retry_connections : 1;

        $retry_wait = Arr::get($this->config, 'retry_wait', 2);
        $retry_wait = $retry_wait > 0 ? (int)$retry_wait : 2;

        $this->unsetProducerClient($key);

        for ($i = 0; $i < $retry_connections; ++$i)
        {
            try
            {
                $this->producerPool[$key] = new Producer($key, $this->config);

                if (!$this->producerPool[$key]->isConnected())
                {
                    logger()->error('Producer isn\'t connected', [
                        'file'   => __FILE__,
                        'line'   => __LINE__,
                        'config' => $this->config,
                    ]);

                    unset($this->producerPool[$key]);
                    sleep($retry_wait);
                    continue;
                }
                break;
            }
            catch (\Exception $e)
            {
                logger()->error('Producer cannot connect to the NSQ', [$e]);
                sleep($retry_wait);
            }
        }

        if (!empty($this->producerPool[$key]))
        {
            return $this->producerPool[$key];
        }

        return null;
    }

    public function unsetProducerClient($key)
    {
        if (isset($this->producerPool[$key]))
        {
            /** @var Producer $producer */
            $producer = $this->producerPool[$key];
            if ($producer->isConnected())
            {
                $producer->close();
            }

            unset($this->consumerPool[$key]);
        }

        return $this;
    }

    public function reconnectConsumerClient($key)
    {
        $retry_connections = Arr::get($this->config, 'retry_num_connections', 10) ?: 1;
        $retry_connections = $retry_connections > 0 ? (int)$retry_connections : 1;

        $retry_wait = Arr::get($this->config, 'retry_wait', 2);
        $retry_wait = $retry_wait > 0 ? (int)$retry_wait : 2;

        $channel = Arr::get($this->config, 'channel', 'web');

        $this->unsetConsumerClient($key);

        for ($i = 0; $i < $retry_connections; ++$i)
        {
            try
            {
                $this->consumerPool[$key] = new Consumer($key, $this->config, $this->topic, $channel);

                if (!$this->consumerPool[$key]->isConnected())
                {
                    logger()->error('Consumer isn\'t connected', [
                        'file'   => __FILE__,
                        'line'   => __LINE__,
                        'topic'  => $this->topic,
                        'config' => $this->config,
                    ]);

                    unset($this->consumerPool[$key]);
                    sleep($retry_wait);
                    continue;
                }
                break;
            }
            catch (\Exception $e)
            {
                logger()->error('Consumer cannot connect to the NSQ topic/channel', [$e]);
                sleep($retry_wait);
            }
        }

        if (!empty($this->consumerPool[$key]))
        {
            return $this->consumerPool[$key];
        }

        return null;
    }

    public function unsetConsumerClient($key)
    {
        if (isset($this->consumerPool[$key]))
        {
            /** @var Consumer $consumer */
            $consumer = $this->consumerPool[$key];
            if ($consumer->isConnected())
            {
                $consumer->close();
            }

            unset($this->consumerPool[$key]);
        }

        return $this;
    }

    public function hasConnectedConcumers()
    {
        return count($this->consumerPool) > 0;
    }

    public function hasConnectedProducers()
    {
        return count($this->producerPool) > 0;
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