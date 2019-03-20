<?php

namespace Merkeleon\Nsq\Queue;

use Carbon\Carbon;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Merkeleon\Nsq\Exception\NsqException;
use Merkeleon\Nsq\Events\FailedPublishMessageToQueueEvent;
use Merkeleon\Nsq\Exception\SubscribeException;
use Merkeleon\Nsq\Exception\WriteToSocketException;
use Merkeleon\Nsq\Jobs\NsqJob;
use Merkeleon\Nsq\Utility\Date;
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
        $this->pool   = new Pool($nsq);
        $this->logger = logger();
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
            $this->fallbackMessage($e, __METHOD__, $message);
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
            $this->fallbackMessage($e, __METHOD__, $bodies);
        }
    }

    public function publishDefer($message, $deferTime)
    {
        try
        {
            $deferTime = Date::convertToInt($deferTime);

            $tunnel = $this->pool->getTunnel();
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
//                $this->logger->info('Ignoring "OK" frame in SUB loop');
            }
            else
            {
//                $this->logger->error("Error/unexpected frame received: ", ['reader' => $reader]);
            }
        }
    }

    /**
     * @param $queue
     * @return Tunnel
     */
    protected function prepareTunnelForReading($queue)
    {
        while ($this->pool->size())
        {
            /** @var Tunnel $tunnel */
            $tunnel = $this->pool->getTunnel();

            try
            {
                return $tunnel->subscribe($queue)
                              ->ready();
            }
            catch (SubscribeException|WriteToSocketException $e)
            {
                try
                {
                    // Try to reconnect to socket
                    // and send ready again
                    return $tunnel->shoutdown()
                                  ->subscribe($queue)
                                  ->ready();
                }
                catch (NsqException $e)
                {
                    $this->pool->removeTunnel($tunnel);
                }
            }
        }

        // Kill process because we have no luck
        // Let process manager do a full process restart
        exit(1);
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
}
