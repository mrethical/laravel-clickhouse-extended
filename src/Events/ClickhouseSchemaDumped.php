<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\Events;

/**
 * Fired after `clickhouse:schema-dump` writes a schema file.
 *
 * Mirrors Illuminate\Database\Events\SchemaDumped.
 */
final class ClickhouseSchemaDumped
{
    public function __construct(
        public readonly string $database,
        public readonly string $path,
    ) {}
}
