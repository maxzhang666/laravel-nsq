<?php

namespace Merkeleon\Nsq\Utility;


use Carbon\Carbon;
use DateInterval;

class Date
{
    public static function convertToInt($delay): int
    {
        if ($delay instanceof Carbon)
        {
            $delay = Carbon::now()
                           ->diffInSeconds($delay);
        }
        else if ($delay instanceof DateInterval)
        {
            $now      = Carbon::now();
            $interval = clone $now;
            $interval->add($delay);

            $delay = $now->diffInSeconds($interval);
        }
        else if (is_numeric($delay) && time() <= $delay)
        {
            $delay -= time();
        }

        return (int)$delay;
    }
}