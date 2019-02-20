<?php

namespace Merkeleon\Nsq\Queue;

use Jiyis\Nsq\Exception\PublishException;
use Jiyis\Nsq\Exception\SubscribeException;
use Jiyis\Nsq\Message\Packet;
use Jiyis\Nsq\Message\Unpack;
use Jiyis\Nsq\Queue\Jobs\NsqJob;
use Jiyis\Nsq\Queue\NsqQueue as JiyisNsqQueue;
use Illuminate\Support\Facades\Config;

class Queue extends JiyisNsqQueue
{
    public const QUEUE_NAME_DEFAULT = 'default';

    /**
     * NsqQueue constructor.
     * @param ClientManager $client
     * @param $consumerJob
     * @param int $retryAfter
     */
    public function __construct(ClientManager $client, $retryAfter = 60)
    {
        parent::__construct($client, null, $retryAfter);
    }

    public function pushRaw($payload, $queue = null, array $options = []): Queue
    {
        if (empty($queue))
        {
            $queue = Config::get('queue.connections.nsq.queue', self::QUEUE_NAME_DEFAULT);
        }

        $cl = Config::get('nsq.options.cl', static::PUB_ONE);

        return $this->publishTo($cl)
                    ->publish($queue, $payload);
    }

    public function publish($topic, $msg, $tries = 1)
    {
        $producerPool = $this->pool->getProducerPool();
        // pick a random
        shuffle($producerPool);

        $success = 0;
        $errors  = [];
        foreach ($producerPool as $producer)
        {
            try
            {
                logger()->info('Try to publish message into the queue', [
                    'topic'    => $topic,
                    'message'  => $msg,
                    'producer' => $producer,
                ]);
                for ($run = 0; $run < $tries; $run++)
                {
                    try
                    {
                        $payload = is_array($msg) ? Packet::mpub($topic, $msg) : Packet::pub($topic, $msg);
                        $producer->send($payload);
                        $frame = Unpack::getFrame($producer->receive());

                        while (Unpack::isHeartbeat($frame))
                        {
                            $producer->send(Packet::nop());
                            $frame = Unpack::getFrame($producer->receive());
                        }

                        if (Unpack::isOK($frame))
                        {
                            $success++;
                        }
                        else
                        {
                            $errors[] = $frame['error'];
                        }

                        break;
                    }
                    catch (\Throwable $e)
                    {
                        logger()->error('Error while sending message into the queue', [
                            'topic'    => $topic,
                            'message'  => $msg,
                            'producer' => $producer,
                            'error'    => $e->getMessage()
                        ]);
                        if ($run >= $tries)
                        {
                            throw $e;
                        }

                        $producer->reconnect();
                    }
                }
            }
            catch (\Throwable $e)
            {
                $errors[] = $e->getMessage();
            }

            if ($success >= $this->pubSuccessCount)
            {
                break;
            }
        }

        if ($success < $this->pubSuccessCount)
        {
            $error = new PublishException(
                sprintf('Failed to publish message; required %s for success, achieved %s. Errors were: %s', $this->pubSuccessCount, $success, implode(', ', $errors))
            );

            logger()->error($error->getMessage());

            throw $error;
        }

        return $this;
    }

    public function pop($queue = null)
    {
        $this->pool->setTopic($queue);
        $this->pool->setChannel();

        try
        {
            if (count($this->pool->getConsumerPool()) < 1)
            {
                $this->pool->connect();
            }

            $response = null;
            foreach ($this->pool->getConsumerPool() as $key => $client)
            {
                // if lost connection  try connect
                //Log::info(socket_strerror($client->getClient()->errCode));
                if (!$client->isConnected())
                {
                    $this->pool->setConsumerPool($key);
                }

                $this->currentClient = $client;

                $data = $this->currentClient->receive();

                // if no message return null
                if ($data == false)
                {
                    continue;
                }

                // unpack message
                $frame = Unpack::getFrame($data);

                if (Unpack::isHeartbeat($frame))
                {
                    //Log::info($key . "sending heartbeat");
                    $this->currentClient->send(Packet::nop());
                }
                elseif (Unpack::isOk($frame))
                {
                    continue;
                }
                elseif (Unpack::isError($frame))
                {
                    continue;
                }
                elseif (Unpack::isMessage($frame))
                {
                    $msg               = \json_decode($frame['message'], true);
                    $this->consumerJob = \unserialize($msg['job']);
                    $rawBody           = $this->adapterNsqPayload($this->consumerJob, $frame);
                    logger()->info("Ready to process job.", ['raw_body' => $rawBody]);

                    $response = new NsqJob($this->container, $this, $rawBody, $queue);
                }
                else
                {
                    logger()->error('Unknown type of message', ['frame' => $frame]);
                }
            }

            $this->refreshClient();

            return $response;

        }
        catch (\Throwable $exception)
        {
            $msg = $exception->getMessage();
            if (strpos($msg, 'Error talking to nsq lookupd via') !== false && strpos($msg, 'lookup?topic=') !== false)
            {
                // Running worker is trying to get data from still not created topic
                // it's correct behaviour, just no one event was fired that initiate topic creation

                logger()->error('It looks like topic wasn\'t created', [$exception]);

                // Force job sleep to prevent NSQ polling
                sleep(60);

                return;
            }
            throw new SubscribeException($exception->getMessage());
        }
    }

    /**
     * refresh nsq client form nsqlookupd result
     */
    protected function refreshClient()
    {
    }
}
