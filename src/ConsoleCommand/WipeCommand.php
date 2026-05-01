<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\ConsoleCommand;

use ClickHouseDB\Client as ClickhouseClient;
use Illuminate\Console\Command;
use Illuminate\Console\ConfirmableTrait;
use Illuminate\Console\Prohibitable;
use Mrethical\LaravelClickhouseExtended\Support\SchemaInspector;
use Symfony\Component\Console\Attribute\AsCommand;

/**
 * Mirrors illuminate/database/Console/WipeCommand (Laravel 12.x), adapted for
 * Clickhouse — drops every view, table, and dictionary in the database, then
 * forgets the bound `ClickhouseClient` singleton.
 */
#[AsCommand(
    name: 'clickhouse:wipe',
    description: 'Drop all tables, views and dictionaries in the Clickhouse database',
)]
class WipeCommand extends Command
{
    use ConfirmableTrait, Prohibitable;

    protected $signature = 'clickhouse:wipe
                {--database= : The Clickhouse database name to wipe (not a Laravel connection name)}
                {--force : Force the operation to run when in production}';

    public function handle(): int
    {
        if ($this->isProhibited() || ! $this->confirmToProceed()) {
            return self::FAILURE;
        }

        $inspector = $this->inspector();

        $this->dropAllViews($inspector);
        $this->dropAllTables($inspector);
        $this->dropAllDictionaries($inspector);

        $this->components->info('Dropped all tables, views and dictionaries successfully.');

        $this->flushClickhouseClient();

        return self::SUCCESS;
    }

    protected function dropAllTables(SchemaInspector $inspector): void
    {
        foreach ($inspector->tables() as $table) {
            $inspector->dropTable($table);
        }
    }

    protected function dropAllViews(SchemaInspector $inspector): void
    {
        foreach ($inspector->views() as $view) {
            $inspector->dropTable($view);
        }
    }

    protected function dropAllDictionaries(SchemaInspector $inspector): void
    {
        foreach ($inspector->dictionaries() as $dictionary) {
            $inspector->dropDictionary($dictionary);
        }
    }

    /**
     * Analog of Laravel's `flushDatabaseConnection`. The smi2/phpclickhouse
     * client is HTTP-per-request and has no persistent connection to close, so
     * forgetting the bound singleton is sufficient — the next resolve will
     * pick up fresh config (e.g. after a parallel-token database switch).
     */
    protected function flushClickhouseClient(): void
    {
        $this->laravel->forgetInstance(ClickhouseClient::class);
    }

    protected function inspector(): SchemaInspector
    {
        $database = $this->option('database');

        return SchemaInspector::forDefault(is_string($database) && $database !== '' ? $database : null);
    }
}
