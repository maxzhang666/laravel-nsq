<?php

namespace Merkeleon\Nsq\Tunnel;


use Illuminate\Support\Arr;
use Merkeleon\Nsq\Exception\NsqException;
use Merkeleon\Nsq\Wire\Writer;
use SplObjectStorage;

class Pool
{
    /** @var SplObjectStorage */
    private $pool;
    private $producerTunnel;
    private $nsq;
    private $size;

    /**
     * Pool constructor.
     * @param $nsq
     */
    public function __construct($nsq)
    {
        $this->pool = new SplObjectStorage;
        $this->size = 0;
        $this->nsq  = $nsq;

        $config = [
            'timeout.connection' => Arr::get($this->nsq, 'timeout.connection', 2),
            'timeout.requeue'    => Arr::get($this->nsq, 'timeout.requeue'),
            'identify'           => Arr::get($this->nsq, 'identify'),
            'blocking'           => Arr::get($this->nsq, 'blocking'),
            'ready'              => Arr::get($this->nsq, 'ready'),
            'attempts.write'     => Arr::get($this->nsq, 'attempts.write', 3),
            'channel'            => Arr::get($this->nsq, 'channel'),
            'queue'              => null,
        ];


        $addresses = Arr::get($nsq, 'nsq.addresses', []);
        [$host, $port] = explode(':', $addresses[array_rand($addresses)]);
        $this->producerTunnel = Tunnel::make($config + ['host' => $host, 'port' => $port], Tunnel::STATE_WRITE);

        $nsqd = [];
        foreach (Arr::get($nsq, 'nsqlookup.addresses', []) as $lookup)
        {
            $nsqd = array_merge($nsqd, $this->callNsqdAddresses($lookup));
        }

        foreach ($nsqd as $url => $port)
        {
            $config['host'] = $url;
            $config['port'] = $port;

            $this->addTunnel(Tunnel::make($config, Tunnel::STATE_READ));
        }

        $this->periodicSocketsPing();
    }

    /**
     * Function pings sockets periodically
     * to keep connections
     */
    public function periodicSocketsPing(): void
    {
        // Function should work if PCNTL is enabled only
        if (!extension_loaded('pcntl'))
        {
            return;
        }

        $heardBeatTime = Arr::get($this->nsq, 'identify.heartbeat_interval', 20000) / 1000;

        pcntl_async_signals(true);
        pcntl_alarm($heardBeatTime / 2);

        $this->mask = pcntl_sigprocmask(SIG_BLOCK, [SIGALRM]);

        pcntl_signal(SIGALRM, function ($signo, $diginfo) use ($heardBeatTime) {
            $size = $this->size();
            if ($size > 0)
            {
                $this->pool->rewind();

                /** @var ConsumerTunnel $tunnel */
                while ($tunnel = $this->pool->current())
                {
                    if ($tunnel->isConnected())
                    {
                        $tunnel->write(Writer::nop());
                    }

                    $this->pool->next();
                }
            }

            if ($this->producerTunnel->isConnected())
            {
                $this->producerTunnel->write(Writer::nop());
            }

            pcntl_alarm($heardBeatTime);
        });

        pcntl_sigprocmask(SIG_UNBLOCK, [SIGALRM]);
    }

    /**
     * @return int
     */
    public function size()
    {
        return $this->pool->count();
    }

    /**
     * @param Tunnel $tunnel
     * @return Pool
     */
    public function addTunnel(Tunnel $tunnel): Pool
    {
        $this->pool->attach($tunnel);
        $this->size++;

        return $this;
    }

    /**
     * @param Tunnel $tunnel
     * @return Pool
     */
    public function removeTunnel(Tunnel $tunnel): Pool
    {
        $tunnel->shutdown();

        $this->pool->detach($tunnel);
        $this->size--;

        return $this;
    }

    /**
     * @return ProducerTunnel|null
     */
    public function getProducer(): ?ProducerTunnel
    {
        return $this->producerTunnel;
    }

    /**
     * @throws \Exception
     * @return Tunnel
     */
    public function getTunnel(): Tunnel
    {
        if ($this->size === 0)
        {
            throw new NsqException('There are no more active tunnels');
        }

        // Get random tunnel from the pool
        $rand = random_int(0, $this->size - 1);

        $this->pool->rewind();
        for ($i = 0; $i < $rand; ++$i)
        {
            $this->pool->next();
        }

        /** @var Tunnel $tunnel */
        $tunnel = $this->pool->current();

        return $tunnel;
    }

    /**
     * @param $lookup
     * @return array
     */
    protected function callNsqdAddresses($lookup): array
    {
        $ch = curl_init();

        // set URL and other appropriate options
        curl_setopt($ch, CURLOPT_URL, $lookup . '/nodes');
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

        // grab URL and pass it to the browser
        $data = curl_exec($ch);

        // close cURL resource, and free up system resources
        curl_close($ch);

        $data = json_decode($data, true);
        if (!$data)
        {
            return [];
        }

        $hosts = [];
        foreach (Arr::get($data, 'producers', []) as $producer)
        {
            $hosts[$producer['broadcast_address']] = $producer['tcp_port'];
        }

        return $hosts;
    }
}