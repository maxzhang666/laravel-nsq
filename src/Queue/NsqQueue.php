<?php

namespace Merkeleon\Nsq\Queue;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Merkeleon\Nsq\Jobs\NsqJob;
use OkStuff\PhpNsq\Message\Message;
use Merkeleon\Nsq\Tunnel\Pool;
use Merkeleon\Nsq\Wire\Reader;
use Exception;
use OkStuff\PhpNsq\Wire\Writer;
use Merkeleon\Nsq\Tunnel\Tunnel;

class NsqQueue extends Queue implements QueueContract
{
    private $pool;
    private $channel;
    private $topic;
    private $reader;
    private $logger;

    public function __construct($nsq)
    {
        $this->reader = new Reader();
        $this->logger = logger();
        $this->pool   = new Pool($nsq);
    }

    public function getLogger()
    {
        return logger();
    }

    public function setChannel($channel)
    {
        $this->channel = $channel;

        return $this;
    }

    public function setTopic($topic)
    {
        $this->topic = $topic;

        return $this;
    }

    public function publish($message)
    {
        try
        {
            $tunnel = $this->pool->getTunnel();
            $tunnel->write(Writer::pub($this->topic, $message));
        }
        catch (Exception $e)
        {
            $this->logger->error("publish error", ['exception' => $e]);
        }
    }

    public function publishMulti(...$bodies)
    {
        try
        {
            $tunnel = $this->pool->getTunnel();
            $tunnel->write(Writer::mpub($this->topic, $bodies));
        }
        catch (Exception $e)
        {
            $this->logger->error("publish error", ['exception' => $e]);
        }
    }

    public function publishDefer($message, $deferTime)
    {
        try
        {
            $tunnel = $this->pool->getTunnel();
            $tunnel->write(Writer::dpub($this->topic, $deferTime, $message));
        }
        catch (Exception $e)
        {
            $this->logger->error("publish error", ['exception' => $e]);
        }
    }

    public function size($queue = null)
    {
    }

    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $queue, $data), $queue);
    }

    public function pushOn($queue, $job, $data = '')
    {
        return $this->push($job, $data, $queue);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $this->setTopic($queue)
             ->publish($payload);
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        $this->setTopic($queue)
             ->publishDefer($this->createPayload($job, $queue, $data), $delay);
    }

    public function laterOn($queue, $delay, $job, $data = '')
    {
        $this->later($delay, $job, $data, $queue);
    }

    public function bulk($jobs, $data = '', $queue = null)
    {
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
        /** @var Tunnel $tunnel */
        $tunnel = $this->pool->getTunnel();
        $tunnel->subscribe($queue)
               ->ready();

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
                    $this->logger->error("Will be requeued: ", ['exception' => $e]);

                    if ($msg)
                    {
                        $tunnel->write(Writer::touch($msg->getId()))
                               ->write(Writer::req(
                                   $msg->getId(),
                                   $tunnel->getConfig()
                                          ->get("defaultRequeueDelay")["default"]
                               ));
                    }
                }
            }
            elseif ($reader->isOk())
            {
                $this->logger->info('Ignoring "OK" frame in SUB loop');
            }
            else
            {
                $this->logger->error("Error/unexpected frame received: ", ['reader' => $reader]);
            }
        }
    }
}
