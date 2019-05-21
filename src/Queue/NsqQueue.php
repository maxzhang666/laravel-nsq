<?php

namespace Merkeleon\Nsq\Queue;

use Exception;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Arr;
use Merkeleon\Nsq\Events\FailedPublishMessageToQueueEvent;
use Merkeleon\Nsq\Exception\
{NsqException};
use Merkeleon\Nsq\Jobs\NsqJob;
use Merkeleon\Nsq\Tunnel\
{Pool, Tunnel};
use Merkeleon\Nsq\Utility\Date;
use Merkeleon\Nsq\Wire\Reader;
use OkStuff\PhpNsq\Message\Message;
use OkStuff\PhpNsq\Wire\Writer;

class NsqQueue extends Queue implements QueueContract
{
    /** @var Pool */
    private $pool;
    private $topic;
    /** @var Reader */
    private $reader;
    private $cfg;

    public function __construct($cfg)
    {
        $this->cfg = $cfg;
        $this->init();
    }

    public function init()
    {
        $this->reader = new Reader();
        $this->pool   = new Pool($this->cfg);
    }

    public function getLogger()
    {
        return logger();
    }

    public function setTopic($topic)
    {
        $this->topic = $this->alignTopic($topic);

        return $this;
    }

    public function publish($message)
    {
        try
        {
            $tunnel = $this->pool->getTunnel();
            $tunnel->connect($this->topic);
            $tunnel->write(Writer::pub($this->topic, $message));
        }
        catch (Exception $e)
        {
            $this->fallbackMessage($e, __METHOD__, $message);
        }
    }

    public function publishMulti(...$bodies)
    {
        try
        {
            $tunnel = $this->pool->getTunnel();
            $tunnel->connect($this->topic);
            $tunnel->write(Writer::mpub($this->topic, $bodies));
        }
        catch (Exception $e)
        {
            $this->fallbackMessage($e, __METHOD__, $bodies);
        }
    }

    public function publishDefer($message, $deferTime)
    {
        try
        {
            $deferTime = Date::convertToInt($deferTime);

            $tunnel = $this->pool->getTunnel();
            $tunnel->connect($this->topic);
            $tunnel->write(Writer::dpub($this->topic, $deferTime, $message));
        }
        catch (Exception $e)
        {
            $this->fallbackMessage($e, __METHOD__, $message);
        }
    }

    public function size($queue = null)
    {
    }

    public function push($job, $data = '', $queue = null)
    {
        $queue = $this->alignTopic($queue);

        return $this->pushRaw($this->createPayload($job, $queue, $data), $queue);
    }

    public function pushOn($queue, $job, $data = '')
    {
        return $this->push($job, $data, $queue);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $queue = $this->alignTopic($queue);
        $this->setTopic($queue)
             ->publish($payload);
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        $queue = $this->alignTopic($queue);
        $this->setTopic($queue)
             ->publishDefer($this->createPayload($job, $queue, $data), $delay);
    }

    public function laterOn($queue, $delay, $job, $data = '')
    {
        $this->later($delay, $job, $data, $queue);
    }

    public function bulk($jobs, $data = '', $queue = null)
    {
        $queue = $this->alignTopic($queue);

        $messages = [];
        foreach ($jobs as $job)
        {
            $messages[] = $this->createPayload($job, $queue, $data);
        }

        $this->setTopic($queue)
             ->publishMulti($messages);
    }

    public function pop($queue = null)
    {
        $queue  = $this->alignTopic($queue);
        $tunnel = $this->prepareTunnelForReading($queue);
        $reader = $this->reader->bindTunnel($tunnel);

        while (true)
        {
            $reader->bindFrame();

            if ($reader->isHeartbeat())
            {
                $tunnel->write(Writer::nop());
            }
            elseif ($reader->isMessage())
            {
                try
                {
                    /** @var Message $msg */
                    $msg = $reader->getMessage();

                    return new NsqJob($this->container, $msg, $tunnel, $this->connectionName, $queue);
                }
                catch (Exception $e)
                {
                    if ($msg)
                    {
                        $tunnel->write(Writer::touch($msg->getId()))
                               ->write(Writer::req($msg->getId(), Arr::get($this->cfg, 'timeout.requeue')));
                    }

                    break;
                }
            }
        }
    }

    /**
     * @param $queue
     * @throws
     * @return Tunnel
     */
    protected function prepareTunnelForReading($queue)
    {
        while (true)
        {
            while ($this->pool->size())
            {
                $tunnel = null;

                try
                {
                    /** @var Tunnel $tunnel */
                    $tunnel = $this->pool->getTunnel();
                    $tunnel->connect($queue);

                    return $tunnel;
                }
                catch (NsqException $e)
                {
                    if ($tunnel)
                    {
                        $this->pool->removeTunnel($tunnel);
                    }
                }
            }

            $this->init();
        }
    }

    /**
     * @param Exception $e
     * @param $message
     */
    protected function fallbackMessage(Exception $e, $publishMethod, $message): void
    {
        $pushSaver = new FailedPublishMessageToQueueEvent();
        $pushSaver->setException($e)
                  ->setFailedAt(date('Y-m-d H:i:s'))
                  ->setPayload($message)
                  ->setQueue($this->topic)
                  ->setPublishMethod($publishMethod);

        event($pushSaver);
    }

    /**
     * @param mixed $queue
     * @return string
     */
    protected function alignTopic($queue)
    {
        if ($queue && is_string($queue))
        {
            return $queue;
        }

        return Arr::get($this->cfg, 'topic', 'default');
    }
}
