<?php

namespace Merkeleon\Nsq\Utility;

use OkStuff\PhpNsq\Utility\Stream as OkStuffStream;
use Exception;

class Stream extends OkStuffStream
{
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
                throw new Exception("stream_select() timeout : " . \json_encode($streamPool) . " after {$timeout} seconds");
            }
            else
            {
                throw new Exception("stream_select() failed : " . \json_encode($streamPool));
            }
        }

        return 0;
    }
}
