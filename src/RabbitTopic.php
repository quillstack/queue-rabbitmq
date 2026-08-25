<?php

declare(strict_types=1);

namespace Quillstack\QueueRabbitMq;

use DateInterval;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Quillstack\Queue\Exceptions\UnknownTopicException;
use Quillstack\Queue\Subscriptions;
use Quillstack\Queue\Topic;

/**
 * A topic RabbitMQ fans out.
 *
 * `Topics\QueueTopic` publishes once per subscriber, because a queue can only be told about one
 * subscriber at a time. A broker does not need telling twice: a message goes to a fanout
 * exchange, and everything bound to it gets a copy. One round trip instead of one per
 * subscriber, and the copying is the broker's problem.
 *
 * This is why `Topic` is an interface. The two do the same thing and share none of the work.
 */
class RabbitTopic implements Topic
{
    private ?AMQPChannel $channel = null;

    /**
     * @var array<string, true>
     */
    private array $bound = [];

    public function __construct(
        private readonly AbstractConnection $connection,
        private readonly Subscriptions $subscriptions,
        private readonly string $prefix = RabbitQueue::PREFIX
    ) {
        //
    }

    /**
     * {@inheritDoc}
     *
     * A delay is per subscriber and a fanout exchange is not, so a delayed publish is done the
     * long way — one message each, held back on its own. Saying it cannot be done would be
     * tidier and less useful.
     */
    public function publish(object $message, string $topic, null|int|DateInterval $delay = null): void
    {
        $queues = $this->subscriptions->queuesFor($topic);

        if ($delay !== null) {
            $queue = new RabbitQueue($this->connection, $this->prefix);

            foreach ($queues as $name) {
                $queue->push($message, $name, $delay);
            }

            return;
        }

        $this->bind($topic, $queues);

        $envelope = new \Quillstack\Queue\Envelope($this->nextId(), $message);

        $this->channel()->basic_publish(
            new AMQPMessage(serialize($envelope), ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]),
            $this->exchangeOf($topic)
        );

        $this->channel()->wait_for_pending_acks();
    }

    /**
     * The exchange, the queues, and what joins them. Declaring is saying what should be true
     * rather than making it so, which is why doing it again costs nothing.
     *
     * @param string[] $queues
     */
    private function bind(string $topic, array $queues): void
    {
        if (isset($this->bound[$topic])) {
            return;
        }

        if ($queues === []) {
            throw new UnknownTopicException("Nothing subscribes to the topic: {$topic}");
        }

        $exchange = $this->exchangeOf($topic);
        $this->channel()->exchange_declare($exchange, 'fanout', false, true, false);

        foreach ($queues as $name) {
            $queue = "{$this->prefix}.{$name}";

            $this->channel()->queue_declare($queue, false, true, false, false);
            $this->channel()->queue_bind($queue, $exchange);
        }

        $this->bound[$topic] = true;
    }

    private function channel(): AMQPChannel
    {
        if ($this->channel === null) {
            $this->channel = $this->connection->channel();
            $this->channel->confirm_select();
        }

        return $this->channel;
    }

    private function exchangeOf(string $topic): string
    {
        return "{$this->prefix}.topic.{$topic}";
    }

    private function nextId(): string
    {
        return bin2hex(random_bytes(8));
    }
}
