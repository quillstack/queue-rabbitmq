<?php

declare(strict_types=1);

namespace Quillstack\QueueRabbitMq\Tests;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use Throwable;

/**
 * Where the broker is, and whether there is one.
 *
 * The tests skip themselves without one so the suite still runs on a machine that has no
 * RabbitMQ. CI has one, and a step which fails if it cannot be reached — otherwise the suite
 * would be green having tested nothing, which is worse than red.
 */
final class Broker
{
    public static function connect(): ?AMQPStreamConnection
    {
        $url = parse_url(getenv('AMQP_URL') ?: 'amqp://guest:guest@127.0.0.1:5672');

        if ($url === false) {
            return null;
        }

        try {
            return new AMQPStreamConnection(
                $url['host'] ?? '127.0.0.1',
                $url['port'] ?? 5672,
                $url['user'] ?? 'guest',
                $url['pass'] ?? 'guest',
                '/',
                false,
                'AMQPLAIN',
                null,
                'en_US',
                2.0
            );
        } catch (Throwable) {
            return null;
        }
    }
}
