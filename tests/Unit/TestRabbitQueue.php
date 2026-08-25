<?php

declare(strict_types=1);

namespace Quillstack\QueueRabbitMq\Tests\Unit;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use Quillstack\QueueRabbitMq\Tests\Broker;
use Quillstack\Clock\FrozenClock;
use Quillstack\Queue\Queue;
use Quillstack\QueueRabbitMq\RabbitQueue;
use Quillstack\QueueRabbitMq\Tests\Mocks\SendWelcomeEmail;
use Quillstack\UnitTests\AssertEqual;
use Quillstack\UnitTests\Types\AssertBoolean;
use Quillstack\UnitTests\Types\AssertNull;

/**
 * Against a real broker. A double would agree to whatever the driver asked of it, and what is
 * worth testing here is what RabbitMQ does — that a message taken and not acknowledged is held
 * rather than gone, and comes back when the connection does.
 *
 * Skipped where there is no broker, so the suite still runs on a machine without one.
 * `AMQP_URL` says where to look.
 */
class TestRabbitQueue
{
    private FrozenClock $clock;
    private string $prefix;

    public function __construct(
        private AssertEqual $assertEqual,
        private AssertNull $assertNull,
        private AssertBoolean $assertBoolean
    ) {
        $this->clock = new FrozenClock();
        $this->prefix = 'quillstack-test-' . getmypid() . '-' . uniqid();
    }

    private function queue(?AMQPStreamConnection $connection = null): ?RabbitQueue
    {
        $connection ??= Broker::connect();

        return $connection === null ? null : new RabbitQueue($connection, $this->prefix, $this->clock);
    }

    public function aMessageGoesThroughTheBroker()
    {
        $queue = $this->queue();

        if ($queue === null) {
            return;
        }

        $queue->push(new SendWelcomeEmail('radek@quillstack.com'));

        $this->assertEqual->equal(1, $queue->size());

        $envelope = $queue->pop();

        $this->assertEqual->equal('radek@quillstack.com', $envelope->message->email);
        $this->assertEqual->equal(1, $envelope->attempts);
        $this->assertBoolean->isTrue($queue->ack($envelope));
        $this->assertEqual->equal(0, $queue->size());

        $queue->forget(Queue::DEFAULT);
    }

    /**
     * The whole reason a broker is worth the trouble: the connection is what holds the
     * message, so a worker which dies gives it back at once rather than after a timeout.
     */
    public function aMessageIsGivenBackWhenTheWorkerGoesAway()
    {
        $connection = Broker::connect();
        $queue = $this->queue($connection);

        if ($queue === null) {
            return;
        }

        $queue->push(new SendWelcomeEmail('taken@example.com'));

        $taken = $queue->pop();
        $this->assertEqual->equal('taken@example.com', $taken->message->email);

        // Held rather than gone: nobody else is offered it while this worker has it.
        $elsewhere = $this->queue();
        $this->assertNull->isNull($elsewhere->pop());

        // The worker dies. Its connection dies with it.
        $connection->close();

        $after = $this->queue();

        $this->assertEqual->equal('taken@example.com', $after->pop()->message->email);

        $after->forget(Queue::DEFAULT);
    }

    /**
     * That an acknowledgement was actually sent cannot be seen from `size()`: the broker
     * reports what is ready, and an unacknowledged message is not ready either. Both look the
     * same until the connection goes — and then an acknowledged message stays gone while a
     * merely-taken one comes back.
     *
     * Without this, breaking `ack()` outright left the whole suite green.
     */
    public function anAcknowledgedMessageDoesNotComeBackWhenTheConnectionDoes()
    {
        $connection = Broker::connect();
        $queue = $this->queue($connection);

        if ($queue === null) {
            return;
        }

        $queue->push(new SendWelcomeEmail('done@example.com'));
        $this->assertBoolean->isTrue($queue->ack($queue->pop()));

        $connection->close();

        $after = $this->queue();

        $this->assertEqual->equal(0, $after->size());
        $this->assertNull->isNull($after->pop());

        $after->forget(Queue::DEFAULT);
    }

    public function anEmptyQueueGivesNothing()
    {
        $queue = $this->queue();

        if ($queue === null) {
            return;
        }

        $this->assertNull->isNull($queue->pop());
        $this->assertEqual->equal(0, $queue->size());

        $queue->forget(Queue::DEFAULT);
    }

    public function messagesComeBackInTheOrderTheyWentIn()
    {
        $queue = $this->queue();

        if ($queue === null) {
            return;
        }

        $queue->push(new SendWelcomeEmail('first@example.com'));
        $queue->push(new SendWelcomeEmail('second@example.com'));

        $first = $queue->pop();
        $second = $queue->pop();

        $this->assertEqual->equal('first@example.com', $first->message->email);
        $this->assertEqual->equal('second@example.com', $second->message->email);

        $queue->ack($first);
        $queue->ack($second);
        $queue->forget(Queue::DEFAULT);
    }

    public function namedQueuesDoNotSeeEachOther()
    {
        $queue = $this->queue();

        if ($queue === null) {
            return;
        }

        $queue->push(new SendWelcomeEmail('mail@example.com'), 'emails');

        $this->assertNull->isNull($queue->pop());
        $this->assertEqual->equal(1, $queue->size('emails'));

        $envelope = $queue->pop('emails');
        $this->assertEqual->equal('mail@example.com', $envelope->message->email);

        $queue->ack($envelope);
        $queue->forget(Queue::DEFAULT, 'emails');
    }

    /**
     * RabbitMQ has no delayed delivery of its own, so this is a queue nothing reads which
     * dead-letters into the real one when the message's time runs out.
     */
    public function aDelayedMessageIsNotGivenOutEarly()
    {
        $queue = $this->queue();

        if ($queue === null) {
            return;
        }

        $queue->push(new SendWelcomeEmail('later@example.com'), Queue::DEFAULT, 1);

        $this->assertNull->isNull($queue->pop());
        $this->assertEqual->equal(1, $queue->size());

        usleep(1_600_000);

        $envelope = $queue->pop();

        $this->assertEqual->equal('later@example.com', $envelope->message->email);

        $queue->ack($envelope);
        $queue->forget(Queue::DEFAULT);
    }

    /**
     * Released, not rejected. A rejected message is the body the broker already had, and the
     * count of attempts lives in that body — so rejecting would hand back a message that had
     * never been tried, for ever.
     */
    public function aReleasedMessageComesBackHavingBeenTried()
    {
        $queue = $this->queue();

        if ($queue === null) {
            return;
        }

        $queue->push(new SendWelcomeEmail('retry@example.com'));

        $queue->release($queue->pop(), 0);

        $again = $queue->pop();

        $this->assertEqual->equal('retry@example.com', $again->message->email);
        $this->assertEqual->equal(2, $again->attempts);

        $queue->ack($again);
        $queue->forget(Queue::DEFAULT);
    }

    public function aFailedMessageIsOutOfTheWayButStillThere()
    {
        $queue = $this->queue();

        if ($queue === null) {
            return;
        }

        $queue->push(new SendWelcomeEmail('broken@example.com'));
        $queue->fail($queue->pop(), 'nothing handles it');

        $this->assertEqual->equal(0, $queue->size());

        $failed = $queue->failed();

        $this->assertEqual->equal(1, count($failed));
        $this->assertEqual->equal('broken@example.com', $failed[0]->message->email);

        // Read rather than consumed: asking twice gives the same answer.
        $this->assertEqual->equal(1, count($queue->failed()));

        $queue->forget(Queue::DEFAULT);
    }
}
