<?php

namespace Merkeleon\Nsq\Tunnel;


use Merkeleon\Nsq\Exception\NsqException;
use Merkeleon\Nsq\Exception\ReadFromSocketException;
use Merkeleon\Nsq\Exception\WriteToSocketException;
use Merkeleon\Nsq\Exception\SocketOpenException;
use Merkeleon\Nsq\Exception\SubscribeException;
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
     * @throws SubscribeException
     */
    public function subscribe($queue, $channel = 'web')
    {
        if ($this->subscribed !== $queue)
        {
            try
            {
                $this->write(Writer::sub($queue, $channel));
                $this->subscribed = $queue;
            }
            catch (Exception $e)
            {
                throw new SubscribeException($e->getMessage(), $e->getCode());
            }
        }

        return $this;
    }

    /**
     * @return $this
     * @throws Send
     */
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
     * @throws NsqException|ReadFromSocketException
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
                try
                {
                    $buffer = Stream::recvFrom($sock, $len);
                }
                catch (Exception $e)
                {
                    throw new ReadFromSocketException($e->getMessage(), $e->getCode());
                }
                $data .= $buffer;
                $len  -= strlen($buffer);
            }
        }

        return $data;
    }

    /**
     * Sends string to the socket
     *
     * @param string $buffer
     * @return Tunnel
     * @throws WriteToSocketException
     */
    public function write($buffer)
    {
        $timeout      = $this->config->get("writeTimeout")["default"];
        $this->writer = [$sock = $this->getSock()];
        while ($buffer != '')
        {
            try
            {
                $writable = Stream::select($this->reader, $this->writer, $timeout);
                if ($writable > 0)
                {
                    $buffer = substr($buffer, Stream::sendTo($sock, $buffer));
                }
            }
            catch (Exception $e)
            {
                throw new WriteToSocketException($e->getMessage(), $e->getCode());
            }
        }

        return $this;
    }

    public function __destruct()
    {
        $this->shoutdown();
    }

    /**
     * Function destroys socket connection
     *
     * @return Tunnel
     */
    public function shoutdown()
    {
        try
        {
            $this->write(Writer::cls());
            fclose($this->getSock());
            $this->sock = null;
        }
        catch (\Exception $e)
        {
            // This exception doesn't matter
        }

        return $this;
    }

    /**
     * @return resource
     * @throws SocketOpenException|WriteToSocketException
     */
    public function getSock()
    {
        if (null === $this->sock)
        {
            try
            {
                $this->sock = Stream::pfopen($this->config->host, $this->config->port);
            }
            catch (Exception $e)
            {
                throw new SocketOpenException($e->getMessage(), $e->getCode());
            }

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