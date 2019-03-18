<?php
/**
 * Created by PhpStorm.
 * User: romik
 * Date: 18.03.19
 * Time: 14:29
 */

namespace Merkeleon\Nsq\Wire;


use OkStuff\PhpNsq\Utility\IntPacker;
use OkStuff\PhpNsq\Wire\Writer as OkStuffWriter;

class Writer extends OkStuffWriter
{
    /**
     * @return string
     */
    public static function identify()
    {
        $data = func_get_arg(0);

        $body = json_encode($data);
        $size = $size = IntPacker::uInt32(strlen($body), true);

        return sprintf("IDENTIFY\n%s", $size.$body);
    }
}