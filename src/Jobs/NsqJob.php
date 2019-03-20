<?php

namespace Merkeleon\Nsq\Jobs;

use Illuminate\Container\Container;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Contracts\Queue\Job as JobContract;
use Merkeleon\Nsq\Utility\Date;
use OkStuff\PhpNsq\Message\Message;
use Merkeleon\Nsq\Tunnel\Tunnel;
use OkStuff\PhpNsq\Wire\Writer;

class NsqJob extends Job implements JobContract
{
    /**
     * @var Message
     */
    protected $msg;

    /**
     * @var Tunnel
     */
    protected $tunnel;

    /**
     * Create a new job instance.
     * NsqJob constructor.
     *
     * @param Container $container
     * @param Message $msg
     * @param Tunnel $tunnel
     * @param string $connectionName
     * @param string $queue
     */
    public function __construct(Container $container, Message $msg, Tunnel $tunnel, $connectionName, $queue)
    {
        $this->msg            = $msg;
        $this->queue          = $queue;
        $this->tunnel         = $tunnel;
        $this->container      = $container;
        $this->connectionName = $connectionName;

        $this->tunnel->write(Writer::touch($msg->getId()));
    }

    /**
     * Get the raw body string for the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return $this->msg->getBody();
    }

    /**
     * Delete the job from the queue.
     *
     * @return void
     */
    public function delete()
    {
        $this->tunnel->write(Writer::fin($this->getJobId()));

        $this->deleted = true;
    }

    /**
     * Release the job back into the queue.
     *
     * @param  int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        $this->tunnel->write(Writer::req($this->getJobId(), Date::convertToInt($delay)));

        $this->released = true;
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts()
    {
        $decoded = $this->payload();

        return ($decoded['attempts'] ?? null) + 1;
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->msg->getId();
    }
}