<?php

namespace Merkeleon\Nsq\Tunnel;


use Illuminate\Support\Arr;
use Merkeleon\Nsq\Exception\NsqException;
use Merkeleon\Nsq\Utility\Stream;
use Merkeleon\Nsq\Wire\Writer;

class ProducerTunnel extends Tunnel
{
    public function connect($queue = null): Tunnel
    {
        if (!$this->connected)
        {
            $host    = $this->config['host'];
            $port    = $this->config['port'];
            $timeout = $this->config['timeout.connection'];

            $this->sock = Stream::pfopen($host, $port, $timeout);

            $this->connected = true;

            stream_set_blocking($this->sock, (bool)$this->config['blocking']);

            $this->write(Writer::MAGIC_V2);
            $this->write(Writer::identify($this->config['identify'] + [
                    'client_id' => 'Producer',
                ]));
        }

        return $this;
    }

    /**
     * Sends string to the socket
     *
     * @param string $buffer
     * @return Tunnel
     * @throws NsqException
     */
    public function write($buffer): Tunnel
    {
        $attempts = $this->config['attempts.write'];

        do
        {
            try
            {
                // Write timeout cannot be greater that double NSQ heardbeat time
                $timeout = Arr::get($this->config, 'identify.heartbeat_interval', 20000) / 1000 * 2;
                while ($buffer !== '')
                {
                    $sock   = $this->getSock();
                    $writer = [$sock];
                    $reader = null;

                    if (Stream::select($reader, $writer, $timeout) > 0)
                    {
                        $wroteBytes = Stream::sendTo($sock, $buffer);
                        $buffer     = substr($buffer, $wroteBytes);
                    }
                    else
                    {
                        // if timeout, let's reconnect anyway there is no available data in the stream
                        $this->shutdown();
                    }
                }

                return $this;
            }
            catch (\Throwable $e)
            {
//                logger()->error($e->getMessage(), ['exception' => $e]);
//                report($e);

                $this->shutdown();
            }
        } while (--$attempts > 0);

        throw new NsqException('ProducerTunnel::write() ended');
    }

    /**
     * @param int $length
     */
    public function read($length = 0)
    {
        throw new NsqException('Producer cannot read from stream');
    }
}