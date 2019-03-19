<?php

namespace Merkeleon\Nsq\Utility;

use Merkeleon\Nsq\Exception\NsqException;
use OkStuff\PhpNsq\Utility\Stream as OkStuffStream;

class Stream extends OkStuffStream
{
    /**
     * @param array $read
     * @param array $write
     * @param $timeout
     * @return int
     * @throws NsqException
     */
    public static function select(array &$read, array &$write, $timeout)
    {
        $streamPool = [
            "read"  => $read,
            "write" => $write,
        ];

        if ($read || $write)
        {
            $except = null;

            $available = @stream_select($read, $write, $except, $timeout === null ? null : 0, $timeout);
            if ($available > 0)
            {
                return $available;
            }
            elseif ($available === 0)
            {
                throw new NsqException("stream_select() timeout : " . \json_encode($streamPool) . " after {$timeout} seconds");
            }
            else
            {
                throw new NsqException("stream_select() failed : " . \json_encode($streamPool));
            }
        }

        return 0;
    }
}
