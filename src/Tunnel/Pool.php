<?php

namespace Merkeleon\Nsq\Tunnel;


use Illuminate\Support\Arr;
use Merkeleon\Nsq\Exception\NsqException;
use SplObjectStorage;

class Pool
{
    /** @var SplObjectStorage */
    private $pool;
    private $nsq;
    private $size;

    public function __construct($nsq)
    {
        $this->pool = new SplObjectStorage;
        $this->size = 0;
        $this->nsq  = $nsq;

        $config = [
            'timeout.connection' => Arr::get($this->nsq, 'timeout.connection'),
            'timeout.read'       => Arr::get($this->nsq, 'timeout.read'),
            'timeout.requeue'    => Arr::get($this->nsq, 'timeout.requeue'),
            'timeout.write'      => Arr::get($this->nsq, 'timeout.write'),
            'identify'           => Arr::get($this->nsq, 'identify'),
            'blocking'           => Arr::get($this->nsq, 'blocking'),
            'ready'              => Arr::get($this->nsq, 'ready'),
            'channel'            => Arr::get($this->nsq, 'channel'),
            'queue'              => null,
        ];

        $nsqd = [];
        foreach (Arr::get($nsq, 'nsqlookup.addresses', []) as $lookup)
        {
            $nsqd = array_merge($nsqd, $this->callNsqdAddresses($lookup));
        }

        foreach ($nsqd as $url => $port)
        {
            $config['host'] = $url;
            $config['port'] = $port;

            $this->addTunnel(new Tunnel($config));
        }
    }

    public function size()
    {
        return $this->pool->count();
    }

    public function addTunnel(Tunnel $tunnel)
    {
        $this->pool->attach($tunnel);
        $this->size++;

        return $this;
    }

    public function removeTunnel(Tunnel $tunnel)
    {
        $tunnel->shoutdown();

        $this->pool->detach($tunnel);
        $this->size--;

        return $this;
    }

    /**
     * @throws \Exception
     * @return Tunnel
     */
    public function getTunnel(): Tunnel
    {
        if ($this->size === 0)
        {
            throw new NsqException('Pool is empty');
        }
        if ($this->size === 1)
        {
            /** @var Tunnel $tunnel */
            $tunnel = $this->pool->current();
        }
        else
        {
            // Get random tunnel from the pool
            $rand = random_int(0, $this->size - 1);

            $this->pool->rewind();
            for ($i = 0; $i < $rand; ++$i)
            {
                $this->pool->next();
            }

            /** @var Tunnel $tunnel */
            $tunnel = $this->pool->current();
        }

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