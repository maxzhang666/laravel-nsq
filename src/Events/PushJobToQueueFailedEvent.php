<?php

namespace Merkeleon\Nsq\Events;


use Illuminate\Queue\SerializesModels;

class PushJobToQueueFailedEvent
{
    use SerializesModels;

    protected $queue;
    protected $payload;
    protected $exception;
    protected $failedAt;
    protected $publishMethod;

    /**
     * @return mixed
     */
    public function getQueue()
    {
        return $this->queue;
    }

    /**
     * @param mixed $queue
     * @return PushJobToQueueFailedEvent
     */
    public function setQueue($queue): PushJobToQueueFailedEvent
    {
        $this->queue = $queue;

        return $this;
    }

    /**
     * @return mixed
     */
    public function getPayload()
    {
        return $this->payload;
    }

    /**
     * @param mixed $payload
     * @return PushJobToQueueFailedEvent
     */
    public function setPayload($payload): PushJobToQueueFailedEvent
    {
        $this->payload = $payload;

        return $this;
    }

    /**
     * @return mixed
     */
    public function getException()
    {
        return $this->exception;
    }

    /**
     * @param mixed $exception
     * @return PushJobToQueueFailedEvent
     */
    public function setException($exception): PushJobToQueueFailedEvent
    {
        $this->exception = $exception;

        return $this;
    }

    /**
     * @return mixed
     */
    public function getFailedAt()
    {
        return $this->failedAt;
    }

    /**
     * @return mixed
     */
    public function getPublishMethod()
    {
        return $this->publishMethod;
    }

    /**
     * @param mixed $publishMethod
     */
    public function setPublishMethod($publishMethod): PushJobToQueueFailedEvent
    {
        $this->publishMethod = $publishMethod;

        return $this;
    }

    /**
     * @param mixed $failedAt
     * @return PushJobToQueueFailedEvent
     */
    public function setFailedAt($failedAt): PushJobToQueueFailedEvent
    {
        $this->failedAt = $failedAt;

        return $this;
    }
}