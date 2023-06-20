<?php

namespace Merkeleon\Nsq\Utility;

use Merkeleon\Nsq\Exception\
{ConnectionException, NsqException, WriteToSocketException};
use Illuminate\Support\Facades\Log;

class Stream
{
    public static function pfopen($host, $port, $timeout)
    {
        try {

        $socket = fsockopen($host, $port, $errno, $errstr, $timeout);
        }catch (\Exception $e){
            Log::error('pfopen error', [$host,$port,$errno,$errstr]);
            throw new ConnectionException("Could not connect to {$host}:{$port} [{$errno}]:[{$errstr}]");
        }
        if (false === $socket)
        {
            throw new ConnectionException("Could not connect to {$host}:{$port} [{$errno}]:[{$errstr}]");
        }

        return $socket;
    }

    public static function sendTo($socket, $buffer)
    {
        $written = @stream_socket_sendto($socket, $buffer);
        if (0 >= $written)
        {
            throw new WriteToSocketException("Could not write " . strlen($buffer) . " bytes to {$socket}");
        }

        return $written;
    }

    public static function recvFrom($socket, $length)
    {
        $buffer = @stream_socket_recvfrom($socket, $length);
        if (empty($buffer))
        {
            throw new WriteToSocketException("Read 0 bytes from {$socket}");
        }

        return $buffer;
    }

    /**
     * @param array $read
     * @param array $write
     * @param $timeout
     * @return int
     * @throws NsqException
     */
    public static function select(array &$read = null, array &$write = null, $timeout = null)
    {
        $streamPool = [
            "read"  => $read,
            "write" => $write,
        ];

        if ($read || $write)
        {
            $except = null;

            $available = @stream_select($read, $write, $except, $timeout);
            if ($available === false)
            {
                throw new NsqException("stream_select() failed : " . \json_encode($streamPool), NsqException::TIMEOUT);
            }
        }

        return $available;
    }
}
