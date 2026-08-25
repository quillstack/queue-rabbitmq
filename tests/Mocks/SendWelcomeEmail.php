<?php

declare(strict_types=1);

namespace Quillstack\QueueRabbitMq\Tests\Mocks;

/**
 * A message carries only what it is about, which is what lets it be written down here and read
 * back out of a broker somewhere else entirely.
 */
final class SendWelcomeEmail
{
    public function __construct(public readonly string $email)
    {
        //
    }
}
