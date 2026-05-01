<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\ConsoleCommand;

use Illuminate\Console\Command;
use Illuminate\Console\ConfirmableTrait;
use Illuminate\Console\Prohibitable;
use Illuminate\Contracts\Events\Dispatcher;
use Mrethical\LaravelClickhouseExtended\Events\ClickhouseDatabaseRefreshed;
use Mrethical\LaravelClickhouseExtended\Support\SchemaInspector;
use Symfony\Component\Console\Attribute\AsCommand;
use Throwable;

/**
 * Mirrors illuminate/database/Console/Migrations/FreshCommand (Laravel 12.x),
 * adapted for Clickhouse — delegates to `clickhouse:wipe` then `clickhouse:migrate`.
 */
#[AsCommand(
    name: 'clickhouse:migrate-fresh',
    description: 'Drop all Clickhouse tables and re-run all migrations',
)]
class MigrateFreshCommand extends Command
{
    use ConfirmableTrait, Prohibitable;

    protected $signature = 'clickhouse:migrate-fresh
                {--database= : The Clickhouse database name to refresh (not a Laravel connection name)}
                {--force : Force the operation to run when in production}
                {--path=* : The path(s) to the migrations files to be executed}
                {--realpath : Indicate any provided migration file paths are pre-resolved absolute paths}
                {--schema-path= : The path to a schema dump file}
                {--step=0 : Number of migrations to run after wipe (0 = all)}';

    public function handle(): int
    {
        if ($this->isProhibited() || ! $this->confirmToProceed()) {
            return self::FAILURE;
        }

        $database = $this->option('database');
        $inspector = SchemaInspector::forDefault(is_string($database) && $database !== '' ? $database : null);

        if ($this->migrationsTableExists($inspector)) {
            $this->newLine();

            $this->components->task(
                'Dropping all tables',
                fn (): bool => $this->callSilent('clickhouse:wipe', array_filter([
                    '--database' => $database,
                    '--force' => true,
                ])) === 0,
            );
        }

        $this->newLine();

        $this->call('clickhouse:migrate', array_filter([
            '--database' => $database,
            '--path' => $this->option('path'),
            '--realpath' => $this->option('realpath'),
            '--schema-path' => $this->option('schema-path'),
            '--force' => true,
            '--step' => $this->option('step'),
        ]));

        if ($this->laravel->bound(Dispatcher::class)) {
            $this->laravel[Dispatcher::class]->dispatch(
                new ClickhouseDatabaseRefreshed($inspector->database()),
            );
        }

        return self::SUCCESS;
    }

    protected function migrationsTableExists(SchemaInspector $inspector): bool
    {
        try {
            return $inspector->tableExists($inspector->migrationsTable());
        } catch (Throwable) {
            return false;
        }
    }
}
