# Quillstack Queue: RabbitMQ

[![Tests](https://github.com/quillstack/queue-rabbitmq/actions/workflows/tests.yml/badge.svg)](https://github.com/quillstack/queue-rabbitmq/actions/workflows/tests.yml)
[![Latest Version](https://img.shields.io/packagist/v/quillstack/queue-rabbitmq.svg)](https://packagist.org/packages/quillstack/queue-rabbitmq)
[![Downloads](https://img.shields.io/packagist/dt/quillstack/queue-rabbitmq.svg)](https://packagist.org/packages/quillstack/queue-rabbitmq)
[![PHP Version](https://img.shields.io/packagist/php-v/quillstack/queue-rabbitmq)](https://packagist.org/packages/quillstack/queue-rabbitmq)
[![StyleCI](https://github.styleci.io/repos/1346582474/shield?branch=main)](https://github.styleci.io/repos/1346582474?branch=main)
[![CodeFactor](https://www.codefactor.io/repository/github/quillstack/queue-rabbitmq/badge)](https://www.codefactor.io/repository/github/quillstack/queue-rabbitmq)
[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=quillstack_queue-rabbitmq&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=quillstack_queue-rabbitmq)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=quillstack_queue-rabbitmq&metric=coverage)](https://sonarcloud.io/summary/new_code?id=quillstack_queue-rabbitmq)
[![Maintainability](https://sonarcloud.io/api/project_badges/measure?project=quillstack_queue-rabbitmq&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=quillstack_queue-rabbitmq)
[![Reliability](https://sonarcloud.io/api/project_badges/measure?project=quillstack_queue-rabbitmq&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=quillstack_queue-rabbitmq)
[![Security](https://sonarcloud.io/api/project_badges/measure?project=quillstack_queue-rabbitmq&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=quillstack_queue-rabbitmq)
[![License](https://img.shields.io/packagist/l/quillstack/queue-rabbitmq)](https://github.com/quillstack/queue-rabbitmq/blob/main/LICENSE)

A RabbitMQ driver for [quillstack/queue](https://github.com/quillstack/queue): the same
`push`, `pop` and `ack`, with a broker holding the messages.

## Why this exists

The drivers in `quillstack/queue` keep messages somewhere the application already had — an
array, a directory, a database table, Redis. That covers most of what an API needs, and the
advice there is to start with the table, because one fewer thing that can be down at three in
the morning is worth more than the microseconds.

This is for when you want what a broker has and the rest of it does not: messages that outlive
the database, other things consuming the same queues, routing, mirroring across nodes.

**It is a separate package because it needs a library.** `quillstack/queue` requires nothing but
PSR interfaces and weighs 512 KB with everything it pulls in; a driver that quietly needed
`php-amqplib` would be a class whose manifest did not say what it needs, failing when it ran
rather than when it was installed. `RedisQueue` stays in the main package because `ext-redis` is
an extension, which is a different thing: it costs nothing to suggest and nothing to skip.

## Requirements

- PHP 8.1 or newer
- RabbitMQ 3.8 or newer
- [quillstack/queue](https://github.com/quillstack/queue) 0.8 or newer

## Installation

```shell
composer require quillstack/queue-rabbitmq
```

## Usage

```php
use PhpAmqpLib\Connection\AMQPStreamConnection;
use Quillstack\QueueRabbitMq\RabbitQueue;

$connection = new AMQPStreamConnection('127.0.0.1', 5672, 'guest', 'guest');
$queue = new RabbitQueue($connection);

$queue->push(new SendWelcomeEmail('radek@quillstack.com'));

$envelope = $queue->pop();
$envelope->attempts;           // 1
$queue->ack($envelope);        // true
$queue->size();                // 0
```

It is a `Quillstack\Queue\Queue`, so everything built on that works over it unchanged —
`Worker`, retries, delays and dead letters included.

### What the broker does instead of a timeout

The other drivers hold a message for a while and give it back when the reservation runs out.
RabbitMQ does not need a clock: `pop` takes a message without acknowledging it, so the broker
holds it against **this connection** and gives it back the moment the connection goes.

A worker that dies takes its connection with it, so the message is back in the queue at once
rather than after a minute. That is the same promise kept better, and it is why there is no
`$visibility` here to set.

### Delays

RabbitMQ has no delayed delivery of its own without a plugin, so a held-back message goes onto
a queue nothing reads, with a time to live, which dead-letters into the real queue when it
expires. Nothing to install, and it is what the broker is for.

One caveat worth knowing: messages on that queue expire in the order they were put there, not
in the order they come due. A message held for an hour in front of one held for a second will
make the second wait. Where delays vary a great deal, the delayed-message-exchange plugin does
this properly.

### Topics

```php
use Quillstack\Queue\Subscriptions;
use Quillstack\QueueRabbitMq\RabbitTopic;

$subscriptions = (new Subscriptions())
    ->subscribe('orders', 'orders.email')
    ->subscribe('orders', 'orders.ledger');

$topic = new RabbitTopic($connection, $subscriptions);

$topic->publish(new OrderPlaced($id), 'orders');
```

`Topics\QueueTopic` in the main package publishes once per subscriber, because a queue can only
be told about one subscriber at a time. A broker does not need telling twice: the message goes
to a fanout exchange and every bound queue gets a copy. One round trip instead of one per
subscriber, and the copying is the broker's problem.

This is the whole reason `Topic` is an interface. The two do the same thing and share none of
the work.

### Where this differs from the other drivers

Said plainly, because a driver which quietly behaves differently is worse than one which does
not exist:

- **`size()` counts what is waiting, not what is in flight.** The other drivers count reserved
  messages too. RabbitMQ does not report those over AMQP — only the management API does, and
  that is a plugin this does not ask anybody to install.
- **A message that comes back after a crash has the attempt count it was published with.** The
  other drivers count an attempt when the message is handed over. RabbitMQ redelivers the body
  it was given, and the count lives in that body. A message released by a worker *is* counted,
  because releasing republishes it; only a crash is invisible.

## Benchmark

Not measured against another RabbitMQ driver for PHP, because the comparison would be between
two ways of calling the same broker over the same library, and the answer would be the network.
What a broker costs is a round trip; what this adds to it is a `serialize()`.

Measuring this against the drivers in `quillstack/queue` would be measuring RabbitMQ against
SQLite, which says nothing about either.

## Tests

```shell
composer test
```

They run against a real broker, because what is worth testing is what RabbitMQ does — that a
message taken and not acknowledged is held rather than gone, and comes back when the connection
does. Without one they skip themselves, so the suite still runs on a machine that has none; CI
has a broker and a step that fails if it cannot be reached, because a green suite that tested
nothing is worse than a red one.

```shell
docker run -d --rm -p 5672:5672 rabbitmq:3-alpine
AMQP_URL=amqp://guest:guest@127.0.0.1:5672 composer test
```

### Static analysis

```shell
composer stan
```

## The rest of Quillstack

This is one component of [Quillstack](https://github.com/quillstack), a PHP framework which is
as simple to use as it is strict about what it does.

- [quillstack/queue](https://github.com/quillstack/queue) — the contract, and the drivers that need no broker
- [quillstack/framework](https://github.com/quillstack/framework) — where a queue is wired in
- [quillstack/clock](https://github.com/quillstack/clock) — what decides when a message is due
- [quillstack/logger](https://github.com/quillstack/logger) — where a worker says what went wrong

## License

MIT — see [LICENSE](https://github.com/quillstack/queue-rabbitmq/blob/main/LICENSE).
