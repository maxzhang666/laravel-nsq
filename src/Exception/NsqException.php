<?php


namespace Merkeleon\Nsq\Exception;

use RuntimeException;

class NsqException extends RuntimeException
{
    public const TIMEOUT = 1;
}