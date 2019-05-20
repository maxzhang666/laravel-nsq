<?php

namespace Merkeleon\Nsq\Tunnel;


use Exception;
use Merkeleon\Nsq\Exception\
{NsqException, ReadFromSocketException, SocketOpenException, SubscribeException, WriteToSocketException};
use Merkeleon\Nsq\Utility\Stream;
use Merkeleon\Nsq\Wire\Writer;

class Tunnel
{
    protected $subscribed;
    protected $sock;
    protected $writer;
    protected $reader;
    /** @var array $config */
    protected $config;

    public function __construct(array $config)
    {
        $this->config     = $config;
        $this->subscribed = false;
    }

    /**
     * @param string $queue
     * @param string $channel
     * @return Tunnel
     * @throws SubscribeException
     */
    public function subscribe()
    {
        if (!$this->subscribed && $this->config['queue'])
        {
            try
            {
                $this->write(Writer::sub($this->config['queue'], $this->config['channel']));
                $this->subscribed = true;
            }
            catch (Exception $e)
            {
                throw new SubscribeException($e->getMessage(), $e->getCode());
            }
        }

        return $this;
    }

    /**
     * @param string $queue
     * @return $this
     */
    public function setQueue($queue)
    {
        $this->config['queue'] = $queue;

        return $this;
    }

    /**
     * @return Tunnel
     * @throws NsqException
     */
    public function ready(): Tunnel
    {
        if ($this->subscribed)
        {
            $this->write(Writer::rdy($this->config['ready']));
        }

        return $this;
    }

    public function connect($queue): Tunnel
    {
        $this->setQueue($queue);
        $this->getSock();

        return $this;
    }

    /**
     * @param int $len
     * @return string
     * @throws NsqException|ReadFromSocketException
     */
    public function read($len = 0)
    {
        try
        {
            $data         = '';
            $this->reader = [$sock = $this->getSock()];
            $this->writer = null;

            while (strlen($data) < $len)
            {
                $readable = Stream::select($this->reader, $this->writer, $this->config['timeout.read']);
                if ($readable > 0)
                {
                    $buffer = Stream::recvFrom($sock, $len);

                    $data .= $buffer;
                    $len  -= strlen($buffer);
                }
            }
        }
        catch (Exception $e)
        {
            $this->shoutdown();

            throw new ReadFromSocketException($e->getMessage(), $e->getCode());
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
        try
        {
            $this->writer = [$sock = $this->getSock()];
            $this->reader = null;

            while ($buffer !== '')
            {
                $writable = Stream::select($this->reader, $this->writer, $this->config['timeout.write']);
                if ($writable > 0)
                {
                    $buffer = substr($buffer, Stream::sendTo($sock, $buffer));
                }

            }
        }
        catch (Exception $e)
        {
            $this->shoutdown();

            throw new WriteToSocketException($e->getMessage(), $e->getCode());
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
        if ($this->sock)
        {
            try
            {
                fclose($this->sock);

                $this->sock       = null;
                $this->subscribed = false;
            }
            catch (\Exception $e)
            {
                // This exception doesn't matter
            }
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
            $this->subscribed = false;

            $host    = $this->config['host'];
            $port    = $this->config['port'];
            $timeout = $this->config['timeout.connection'];

            $this->sock = Stream::pfopen($host, $port, $timeout);

            stream_set_blocking($this->sock, (bool)$this->config['blocking']);

            $this->write(Writer::MAGIC_V2);
            $this->write(Writer::identify($this->config['identify']));

            $this->subscribe()
                 ->ready();
        }

        return $this->sock;
    }
}