<?php

namespace Merkeleon\Nsq\Tunnel;


use Illuminate\Support\Arr;
use Merkeleon\Nsq\Exception\NsqException;
use Merkeleon\Nsq\Utility\Stream;
use Merkeleon\Nsq\Wire\Writer;

class ConsumerTunnel extends ProducerTunnel
{
    protected $subscribed;
    protected $reader;

    /**
     * @return ConsumerTunnel
     * @throws NsqException
     */
    public function subscribe(): ConsumerTunnel
    {
        if (!$this->subscribed)
        {
            try
            {
                $this->write(Writer::sub($this->config['queue'], $this->config['channel']));
                $this->subscribed = true;
            }
            catch (\Exception $e)
            {
                throw new NsqException($e->getMessage(), $e->getCode());
            }
        }

        return $this;
    }

    /**
     * @param string $queue
     * @return ConsumerTunnel
     */
    public function setQueue($queue): ConsumerTunnel
    {
        $this->config['queue'] = $queue;

        return $this;
    }

    /**
     * @return ConsumerTunnel
     * @throws NsqException
     */
    public function ready(): ConsumerTunnel
    {
        if ($this->subscribed)
        {
            $this->write(Writer::rdy($this->config['ready']));
        }

        return $this;
    }

    /**
     * @param string|null $queue
     * @return Tunnel
     */
    public function connect($queue = null): Tunnel
    {
        if ($this->isConnected())
        {
            return $this;
        }

        if ($queue)
        {
            $this->setQueue($queue);
        }

        $host    = $this->config['host'];
        $port    = $this->config['port'];
        $timeout = $this->config['timeout.connection'];

        $this->sock = Stream::pfopen($host, $port, $timeout);

        stream_set_blocking($this->sock, (bool)$this->config['blocking']);

        $this->write(Writer::MAGIC_V2);
        $this->write(Writer::identify($this->config['identify'] + [
                'client_id' => 'Consumer: ' . $this->config['queue'],
            ]));

        $this->subscribed = false;
        $this->subscribe()
             ->ready();

        $this->connected = true;

        return $this;
    }

    /**
     * @return Tunnel
     */
    public function shutdown(): Tunnel
    {
        parent::shutdown();

        $this->subscribed = false;

        return $this;
    }

    /**
     * @param int $len
     * @return string
     * @throws NsqException
     */
    public function read($len = 0)
    {
        try
        {
            $data    = '';
            $sock    = $this->getSock();
            $reader  = [$sock];
            $writer  = null;
            $timeout = Arr::get($this->config, 'identify.heartbeat_interval', 20000) / 1000 * 2;

            while (strlen($data) < $len)
            {
                $readable = Stream::select($reader, $writer, $timeout);
                if ($readable > 0)
                {
                    $buffer = Stream::recvFrom($sock, $len);

                    $data .= $buffer;
                    $len  -= strlen($buffer);
                }
                else
                {
                    // if timeout, let's reconnect anyway there is no available data in the stream
                    $this->shutdown();
                    $sock = $this->getSock();
                }
            }
        }
        catch (\Throwable $e)
        {
//            logger()->error($e->getMessage(), ['exception' => $e]);
//            report($e);

            $this->shutdown();

            throw new NsqException($e->getMessage(), $e->getCode());
        }

        return $data;
    }
}