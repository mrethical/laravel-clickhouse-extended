<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\Events;

/**
 * Fired after `clickhouse:migrate` loads a schema dump.
 *
 * Mirrors Illuminate\Database\Events\SchemaLoaded.
 */
final class ClickhouseSchemaLoaded
{
    public function __construct(
        public readonly string $database,
        public readonly string $path,
    ) {}
}
