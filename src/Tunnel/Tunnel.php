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
    protected $identify;

    public function __construct(Config $config, $identify)
    {
        $this->identify = $identify;
        $this->config   = $config;
        $this->writer   = [];
        $this->reader   = [];
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
            // Run socket initialization
            $this->getSock($this->identify);
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
     * @return Tunnel
     * @throws Send
     */
    public function ready(): Tunnel
    {
        if ($this->subscribed === null)
        {
            throw new NsqException('Tunnel should be subscribed first');
        }

        $this->write(Writer::rdy(1));

        return $this;
    }

    /**
     * @return Config
     */
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
        $this->reader = [$sock = $this->getSock($this->identify)];

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
        $this->writer = [$sock = $this->getSock($this->identify)];

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
        if ($this->sock)
        {
            try
            {
                fclose($this->sock);

                $this->sock       = null;
                $this->subscribed = null;
            }
            catch (\Exception $e)
            {
                // This exception doesn't matter
            }
        }

        return $this;
    }

    /**
     * @param array $identity
     * @return resource
     * @throws SocketOpenException|WriteToSocketException
     */
    public function getSock($identity)
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
            $this->write(Writer::identify($identity));
        }

        return $this->sock;
    }
}