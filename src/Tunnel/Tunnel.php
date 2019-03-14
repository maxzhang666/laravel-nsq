<?php

namespace Merkeleon\Nsq\Tunnel;


use Merkeleon\Nsq\Utility\Stream;
use OkStuff\PhpNsq\Tunnel\Config;
use Merkeleon\Nsq\Wire\Writer;
use Exception;

class Tunnel
{
    protected $subscribed;
    protected $config;
    protected $sock;
    protected $writer;
    protected $reader;

    public function __construct(Config $config)
    {
        $this->config = $config;
        $this->writer = [];
        $this->reader = [];
    }

    /**
     * @param $queue
     * @return Tunnel
     * @throws Exception
     */
    public function subscribe($queue, $channel = 'web')
    {
        if ($this->subscribed !== $queue)
        {
            $this->write(Writer::sub($queue, $channel));
            $this->subscribed = $queue;
        }

        return $this;
    }

    public function ready()
    {
        $this->write(Writer::rdy(1));

        return $this;
    }

    public function getConfig()
    {
        return $this->config;
    }

    /**
     * @param int $len
     * @return string
     * @throws Exception
     */
    public function read($len = 0)
    {
        $data         = '';
        $timeout      = $this->config->get("readTimeout")["default"];
        $this->reader = [$sock = $this->getSock()];
        while (strlen($data) < $len)
        {
            $readable = Stream::select($this->reader, $this->writer, $timeout);
            if ($readable > 0)
            {
                $buffer = Stream::recvFrom($sock, $len);
                $data   .= $buffer;
                $len    -= strlen($buffer);
            }
        }

        return $data;
    }

    /**
     * @param $buffer
     * @return $this
     * @throws Exception
     */
    public function write($buffer)
    {
        $timeout      = $this->config->get("writeTimeout")["default"];
        $this->writer = [$sock = $this->getSock()];
        while (strlen($buffer) > 0)
        {
            $writable = Stream::select($this->reader, $this->writer, $timeout);
            if ($writable > 0)
            {
                $buffer = substr($buffer, Stream::sendTo($sock, $buffer));
            }
        }

        return $this;
    }

    public function __destruct()
    {
        try
        {
            $this->write(Writer::cls());
            fclose($this->getSock());
        }
        catch (\Exception $e)
        {
        }
    }

    /**
     * @return resource
     * @throws \Exception
     */
    public function getSock()
    {
        if (null === $this->sock)
        {
            $this->sock = Stream::pfopen($this->config->host, $this->config->port);

            if (false === $this->config->get("blocking"))
            {
                stream_set_blocking($this->sock, 0);
            }

            $this->write(Writer::MAGIC_V2);
        }

        return $this->sock;
    }

    /**
     * @param mixed $identity
     * @return $this
     * @throws Exception
     */
    public function setIdentify($identity)
    {
        $this->write(Writer::identify($identity));

        return $this;
    }
}