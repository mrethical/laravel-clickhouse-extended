<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\Events;

/**
 * Fired after `clickhouse:migrate-fresh` finishes wiping + re-migrating.
 *
 * Mirrors Illuminate\Database\Events\DatabaseRefreshed.
 */
final class ClickhouseDatabaseRefreshed
{
    public function __construct(
        public readonly string $database,
    ) {}
}
