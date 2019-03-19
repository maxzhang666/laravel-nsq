<?php

namespace Merkeleon\Nsq\Events;


use Illuminate\Queue\SerializesModels;

class FailedPublishMessageToQueueEvent
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
     * @return FailedPublishMessageToQueueEvent
     */
    public function setQueue($queue): FailedPublishMessageToQueueEvent
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
     * @return FailedPublishMessageToQueueEvent
     */
    public function setPayload($payload): FailedPublishMessageToQueueEvent
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
     * @return FailedPublishMessageToQueueEvent
     */
    public function setException($exception): FailedPublishMessageToQueueEvent
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
    public function setPublishMethod($publishMethod): FailedPublishMessageToQueueEvent
    {
        $this->publishMethod = $publishMethod;

        return $this;
    }

    /**
     * @param mixed $failedAt
     * @return FailedPublishMessageToQueueEvent
     */
    public function setFailedAt($failedAt): FailedPublishMessageToQueueEvent
    {
        $this->failedAt = $failedAt;

        return $this;
    }
}