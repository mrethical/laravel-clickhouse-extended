<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\Events;

/**
 * Fired after `clickhouse:schema-dump --prune` deletes the migration files.
 *
 * Mirrors Illuminate\Database\Events\MigrationsPruned.
 */
final class ClickhouseMigrationsPruned
{
    public function __construct(
        public readonly string $database,
        public readonly string $path,
    ) {}
}
