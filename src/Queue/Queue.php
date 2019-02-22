<?php

namespace Merkeleon\Nsq\Queue;

use Jiyis\Nsq\Exception\PublishException;
use Jiyis\Nsq\Exception\SubscribeException;
use Jiyis\Nsq\Message\Packet;
use Jiyis\Nsq\Message\Unpack;
use Jiyis\Nsq\Queue\Jobs\NsqJob;
use Jiyis\Nsq\Queue\NsqQueue as JiyisNsqQueue;
use Illuminate\Support\Facades\Config;
use Merkeleon\Nsq\Monitor\Consumer;
use Exception;

class Queue extends JiyisNsqQueue
{
    public const QUEUE_NAME_DEFAULT = 'default';

    /** @var ClientManager */
    protected $pool;

    protected $counter = 0;
    protected $rdy     = 30;

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

    /**
     * @param string $topic
     * @param array|string $msg
     * @param int $tries
     * @return $this|JiyisNsqQueue
     * @throws Exception
     */
    public function publish($topic, $msg, $tries = 1)
    {
        /** @var ClientManager $pool */
        $pool = $this->pool;
        if (!$pool->hasConnectedProducers())
        {
            $pool->setUpProducers();
        }

        $producerPool = $pool->getProducerPool();

        // pick a random
        shuffle($producerPool);

        $success = 0;
        $errors  = [];
        foreach ($producerPool as $key => $producer)
        {
            if (!$producer->isConnected())
            {
                $producer = $pool->reconnectProducerClient($key);
                if (!$producer)
                {
                    throw new Exception('Producer isn\'t connected');
                }
            }
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

                        $this->pool->reconnectProducerClient($key);
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
        /** @var ClientManager $pool */
        $pool = $this->pool;

        $pool->setTopic($queue);
        $pool->setChannel();

        try
        {
            if (!$pool->hasConnectedConcumers())
            {
                $pool->setUpConsumers();
            }

            $response = null;
            foreach ($this->pool->getConsumerPool() as $key => $client)
            {
                // if lost connection  try connect
                if (!$client->isConnected())
                {
                    $client = $this->pool->reconnectConsumerClient($key);
                    if (!$client)
                    {
                        throw new Exception('Consumer isn\'t connected');
                    }
                }

                $this->currentClient = $client;

                $data = $this->currentClient->receive();

                $this->updateConsumerBuffer($client);

                // if no message return null
                if ($data == false)
                {
                    continue;
                }

                // unpack message
                $frame = Unpack::getFrame($data);

                if (Unpack::isHeartbeat($frame))
                {
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

//            $this->refreshClient();

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
        // check connect time
//        $connectTime = $this->pool->getConnectTime();
//        $currentTime = time();
//
//        if ($currentTime - $connectTime >= 300) // 5 min
//        {
//            foreach ($this->pool->getConsumerPool() as $key => $client)
//            {
//                $this->pool->reconnectConsumerClient($key);
//            }
//            logger()->info("refresh nsq client success.");
//
//            $this->pool->setConnectTime($currentTime);
//        }
    }

    /**
     * @param Consumer $client
     */
    protected function updateConsumerBuffer(Consumer $client)
    {
        $this->counter++;

        $current = $this->rdy - $this->counter;
        $low     = 1;

        if ($current <= $low)
        {
            $this->counter = 0;
            $client->send(Packet::rdy($this->rdy));
        }
    }
}
