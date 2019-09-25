<?php

namespace Merkeleon\Nsq\Tunnel;


abstract class Tunnel
{
    public const STATE_WRITE = 1;
    public const STATE_READ  = 2;

    /** @var bool */
    protected $connected;
    /** @var resource */
    protected $sock;
    /** @var array $config */
    protected $config;

    protected $mask;

    public static function make(array $config, $state = self::STATE_READ)
    {
        if ($state === self::STATE_READ)
        {
            return new ConsumerTunnel($config);
        }
        if ($state === self::STATE_WRITE)
        {

            return new ProducerTunnel($config);
        }
    }

    public function __construct(array $config)
    {
        $this->config    = $config;
        $this->connected = false;
    }

    public function __destruct()
    {
        $this->shutdown();
    }

    /**
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->connected;
    }

    /**
     * Function destroys socket connection
     *
     * @return Tunnel
     */
    public function shutdown()
    {
        if ($this->isConnected() && is_resource($this->sock))
        {
            try
            {
                fclose($this->sock);
            }
            catch (\Exception $e)
            {
                // This exception doesn't matter
            }
        }

        $this->connected = false;
        $this->sock      = null;

        return $this;
    }

    public function getSock()
    {
        if ($this->sock === null || !is_resource($this->sock) || feof($this->sock))
        {
            $this->connected = false;
            $this->connect();
        }

        return $this->sock;
    }

    abstract public function connect($queue = null);

    abstract public function write($msg);

    abstract public function read($length = 0);
}