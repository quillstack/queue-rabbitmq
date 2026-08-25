<?php

declare(strict_types=1);

namespace Quillstack\QueueRabbitMq;

use DateInterval;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Psr\Clock\ClockInterface;
use Quillstack\Clock\SystemClock;
use Quillstack\Queue\Delay;
use Quillstack\Queue\Envelope;
use Quillstack\Queue\Queue;
use Throwable;

/**
 * A queue kept by RabbitMQ.
 *
 * The other drivers keep messages somewhere the application already had — an array, a
 * directory, a table, Redis. This one hands the whole job to a broker, which is worth doing
 * when you want what a broker has and the rest of this does not: messages that outlive the
 * database, routing, mirroring across nodes, and other things consuming the same queues.
 *
 * The reservation is the broker's. `pop` takes a message without acknowledging it, so RabbitMQ
 * holds it against this connection and gives it back the moment the connection goes — which is
 * sooner and surer than a timeout, because a dead worker's connection dies with it.
 */
class RabbitQueue implements Queue
{
    /**
     * @var string
     */
    public const PREFIX = 'quillstack';

    /**
     * @var string
     */
    private const FAILED = 'failed';

    private ?AMQPChannel $channel = null;

    private ClockInterface $clock;

    /**
     * @var array<string, true>
     */
    private array $declared = [];

    public function __construct(
        private readonly AbstractConnection $connection,
        private readonly string $prefix = self::PREFIX,
        ?ClockInterface $clock = null
    ) {
        $this->clock = $clock ?? new SystemClock();
    }

    /**
     * {@inheritDoc}
     */
    public function push(object $message, string $queue = self::DEFAULT, null|int|DateInterval $delay = null): Envelope
    {
        $now = $this->now();

        $envelope = new Envelope($this->nextId(), $message, $queue, 0, Delay::until($delay, $now));

        $this->publish($envelope, $now);

        return $envelope;
    }

    /**
     * {@inheritDoc}
     */
    public function pop(string $queue = self::DEFAULT): ?Envelope
    {
        $this->declare($queue);

        while (true) {
            $message = $this->channel()->basic_get($this->nameOf($queue));

            if (!$message instanceof AMQPMessage) {
                return null;
            }

            $envelope = $this->unpack($message->getBody());

            if ($envelope === null) {
                // Unreadable. Taking it out of the way is better than handing it back and
                // being given it again for ever.
                $this->channel()->basic_ack($message->getDeliveryTag());

                continue;
            }

            return $envelope->reservedAs((string) $message->getDeliveryTag());
        }
    }

    /**
     * {@inheritDoc}
     */
    public function ack(Envelope $envelope): bool
    {
        if ($envelope->reservation === '') {
            return false;
        }

        try {
            $this->channel()->basic_ack((int) $envelope->reservation);
        } catch (Throwable) {
            // A delivery this channel is not holding — the connection it belonged to is gone,
            // and so is the right to say anything about the message.
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoc}
     *
     * Acknowledged and published again rather than rejected back onto the queue. A rejected
     * message is the body the broker already had, and the count of attempts lives in that
     * body — so rejecting would hand back a message that had never been tried, for ever.
     */
    public function release(Envelope $envelope, null|int|DateInterval $delay = null): Envelope
    {
        $now = $this->now();
        $released = $envelope->retryAt(Delay::until($delay, $now));

        $this->publish($released, $now);
        $this->ack($envelope);

        return $released;
    }

    /**
     * {@inheritDoc}
     */
    public function fail(Envelope $envelope, string $reason): void
    {
        $this->declare(self::FAILED);
        $this->send($this->nameOf(self::FAILED), serialize($envelope), null);
        $this->ack($envelope);
    }

    /**
     * {@inheritDoc}
     *
     * What RabbitMQ reports over AMQP is what is ready. A message a worker is holding is not
     * counted, because the broker does not say over this protocol how many those are — only
     * the management API does, and that is a plugin this does not ask anybody to install.
     */
    public function size(string $queue = self::DEFAULT): int
    {
        $this->declare($queue);

        return $this->countOf($this->nameOf($queue)) + $this->countOf($this->delayNameOf($queue));
    }

    /**
     * {@inheritDoc}
     *
     * Read without being consumed: each is taken and put straight back, so asking twice gives
     * the same answer. The order they come back in is the broker's, not the order they failed.
     */
    public function failed(): array
    {
        $this->declare(self::FAILED);

        $name = $this->nameOf(self::FAILED);
        $failed = [];
        $seen = [];

        while (($message = $this->channel()->basic_get($name)) instanceof AMQPMessage) {
            $seen[] = $message->getDeliveryTag();
            $envelope = $this->unpack($message->getBody());

            if ($envelope !== null) {
                $failed[] = $envelope;
            }
        }

        foreach ($seen as $tag) {
            $this->channel()->basic_nack($tag, false, true);
        }

        return $failed;
    }

    /**
     * Everything this driver made, taken away again. For a test, and for nothing else.
     */
    public function forget(string ...$queues): void
    {
        foreach ([...$queues, self::FAILED] as $queue) {
            $this->channel()->queue_delete($this->nameOf($queue));
            $this->channel()->queue_delete($this->delayNameOf($queue));
        }

        $this->declared = [];
    }

    /**
     * Where a message goes: straight onto the queue, or onto the one that holds it back.
     */
    private function publish(Envelope $envelope, int $now): void
    {
        $this->declare($envelope->queue);

        $waitFor = $envelope->availableAt - $now;

        if ($waitFor <= 0) {
            $this->send($this->nameOf($envelope->queue), serialize($envelope), null);

            return;
        }

        // RabbitMQ has no delayed delivery of its own without a plugin. A message with a time
        // to live on a queue nothing reads, which dead-letters into the real one, is how it is
        // done with what the broker has.
        $this->send($this->delayNameOf($envelope->queue), serialize($envelope), $waitFor * 1000);
    }

    private function send(string $queue, string $body, ?int $expiresIn): void
    {
        $properties = ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT];

        if ($expiresIn !== null) {
            $properties['expiration'] = (string) $expiresIn;
        }

        $this->channel()->basic_publish(new AMQPMessage($body, $properties), '', $queue);

        // Waited for, so a push which returns is a push the broker has, and asking the size
        // straight afterwards is not a race.
        $this->channel()->wait_for_pending_acks();
    }

    private function declare(string $queue): void
    {
        if (isset($this->declared[$queue])) {
            return;
        }

        $name = $this->nameOf($queue);

        $this->channel()->queue_declare($name, false, true, false, false);
        $this->channel()->queue_declare($this->delayNameOf($queue), false, true, false, false, false, new AMQPTable([
            'x-dead-letter-exchange' => '',
            'x-dead-letter-routing-key' => $name,
        ]));

        $this->declared[$queue] = true;
    }

    private function countOf(string $name): int
    {
        $declared = $this->channel()->queue_declare($name, true);

        return is_array($declared) && isset($declared[1]) ? (int) $declared[1] : 0;
    }

    private function channel(): AMQPChannel
    {
        if ($this->channel === null) {
            $this->channel = $this->connection->channel();
            $this->channel->confirm_select();
        }

        return $this->channel;
    }

    private function unpack(string $body): ?Envelope
    {
        $envelope = @unserialize($body);

        return $envelope instanceof Envelope ? $envelope : null;
    }

    private function nameOf(string $queue): string
    {
        return "{$this->prefix}.{$queue}";
    }

    private function delayNameOf(string $queue): string
    {
        return "{$this->prefix}.{$queue}.delayed";
    }

    private function now(): int
    {
        return $this->clock->now()->getTimestamp();
    }

    private function nextId(): string
    {
        return bin2hex(random_bytes(8));
    }
}
