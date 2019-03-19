<?php

namespace Merkeleon\Nsq\Tunnel;


use Illuminate\Support\Arr;
use Merkeleon\Nsq\Exception\NsqException;
use OkStuff\PhpNsq\Tunnel\Config;
use SplObjectStorage;

class Pool
{
    /** @var SplObjectStorage */
    private $pool;
    private $nsq;

    public function __construct($nsq)
    {
        $this->pool = new SplObjectStorage;
        $this->nsq  = $nsq;

        $nsqd = [];
        foreach (Arr::get($nsq, 'nsqlookup.addresses', []) as $lookup)
        {
            $nsqd = array_merge($nsqd, $this->callNsqdAddresses($lookup));
        }

        if (!$nsqd)
        {
            foreach (Arr::get($nsq, 'nsq.addresses', []) as $value)
            {
                [$url, $port] = explode(':', $value);
                $nsqd[$url] = $port;
            }
        }

        foreach ($nsqd as $url => $port)
        {
            $this->addTunnel(new Tunnel(new Config($url, $port)));
        }
    }

    public function size()
    {
        return $this->pool->count();
    }

    public function addTunnel(Tunnel $tunnel)
    {
        $this->pool->attach($tunnel);

        return $this;
    }

    public function removeTunnel(Tunnel $tunnel)
    {
        $this->pool->detach($tunnel);

        return $this;
    }

    /**
     * @return Tunnel
     */
    public function getTunnel()
    {
        if ($this->pool->count() < 1)
        {
            throw new NsqException('Pool is empty');
        }

        $rand = random_int(0, $this->pool->count() - 1);

        $this->pool->rewind();
        for ($i = 0; $i < $rand; ++$i)
        {
            $this->pool->next();
        }

        $tunnel = $this->pool->current();
        $tunnel->setIdentify($this->nsq['identify']);

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
            $hosts[$producer['hostname']] = $producer['tcp_port'];
        }

        return $hosts;
    }
}