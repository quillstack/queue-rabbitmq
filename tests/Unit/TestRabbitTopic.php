<?php

declare(strict_types=1);

namespace Quillstack\QueueRabbitMq\Tests\Unit;

use Quillstack\Queue\Exceptions\UnknownTopicException;
use Quillstack\Queue\Subscriptions;
use Quillstack\Queue\Topic;
use Quillstack\QueueRabbitMq\RabbitQueue;
use Quillstack\QueueRabbitMq\RabbitTopic;
use Quillstack\QueueRabbitMq\Tests\Broker;
use Quillstack\QueueRabbitMq\Tests\Mocks\SendWelcomeEmail;
use Quillstack\UnitTests\AssertEqual;
use Quillstack\UnitTests\AssertExceptions;
use Quillstack\UnitTests\Types\AssertBoolean;

/**
 * `Topics\QueueTopic` publishes once per subscriber because a queue can only be told about one
 * at a time. A broker does not need telling twice — and that difference is the reason `Topic`
 * is an interface rather than a class.
 */
class TestRabbitTopic
{
    private string $prefix;

    public function __construct(
        private AssertEqual $assertEqual,
        private AssertBoolean $assertBoolean,
        private AssertExceptions $assertExceptions
    ) {
        $this->prefix = 'quillstack-topic-' . getmypid() . '-' . uniqid();
    }

    private function subscriptions(): Subscriptions
    {
        return (new Subscriptions())
            ->subscribe('orders', 'orders.email')
            ->subscribe('orders', 'orders.ledger');
    }

    /**
     * One publish, and the broker puts a copy on every bound queue.
     */
    public function oneMessageReachesEverySubscriber()
    {
        $connection = Broker::connect();

        if ($connection === null) {
            return;
        }

        $topic = new RabbitTopic($connection, $this->subscriptions(), $this->prefix);
        $queue = new RabbitQueue($connection, $this->prefix);

        $this->assertBoolean->isTrue($topic instanceof Topic);

        $topic->publish(new SendWelcomeEmail('ada@example.com'), 'orders');

        $this->assertEqual->equal(1, $queue->size('orders.email'));
        $this->assertEqual->equal(1, $queue->size('orders.ledger'));

        $email = $queue->pop('orders.email');
        $ledger = $queue->pop('orders.ledger');

        $this->assertEqual->equal('ada@example.com', $email->message->email);
        $this->assertEqual->equal('ada@example.com', $ledger->message->email);

        $queue->ack($email);
        $queue->ack($ledger);
        $queue->forget('orders.email', 'orders.ledger');
    }

    /**
     * Each subscriber has its own copy, so what one does with it is nobody else's business.
     */
    public function whatOneSubscriberDoesDoesNotReachTheOthers()
    {
        $connection = Broker::connect();

        if ($connection === null) {
            return;
        }

        $topic = new RabbitTopic($connection, $this->subscriptions(), $this->prefix);
        $queue = new RabbitQueue($connection, $this->prefix);

        $topic->publish(new SendWelcomeEmail('ada@example.com'), 'orders');

        $queue->fail($queue->pop('orders.email'), 'the mailer is down');

        $this->assertEqual->equal(0, $queue->size('orders.email'));
        $this->assertEqual->equal(1, $queue->size('orders.ledger'));

        $ledger = $queue->pop('orders.ledger');
        $this->assertEqual->equal('ada@example.com', $ledger->message->email);

        $queue->ack($ledger);
        $queue->forget('orders.email', 'orders.ledger');
    }

    public function publishingToATopicNobodySubscribesToIsRefused()
    {
        $connection = Broker::connect();

        if ($connection === null) {
            $this->assertBoolean->isTrue(true);

            return;
        }

        $topic = new RabbitTopic($connection, $this->subscriptions(), $this->prefix);

        $this->assertExceptions->expect(UnknownTopicException::class);

        $topic->publish(new SendWelcomeEmail('ada@example.com'), 'oredrs');
    }
}
