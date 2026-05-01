<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\ConsoleCommand;

use ClickHouseDB\Client as ClickhouseClient;
use Cog\Laravel\Clickhouse\Migration\Migrator;
use Illuminate\Console\Command;
use Illuminate\Console\ConfirmableTrait;
use Illuminate\Contracts\Config\Repository as AppConfigRepositoryInterface;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Filesystem\Filesystem;
use Mrethical\LaravelClickhouseExtended\Events\ClickhouseSchemaLoaded;
use Mrethical\LaravelClickhouseExtended\Support\SchemaInspector;
use Symfony\Component\Console\Attribute\AsCommand;
use Throwable;

/**
 * Run pending Clickhouse migrations, optionally bootstrapping from a schema dump.
 *
 * Mirrors illuminate/database/Console/Migrations/MigrateCommand (Laravel 12.x).
 * Replaces the upstream `cybercog/laravel-clickhouse` `clickhouse:migrate` —
 * the override is wired explicitly via `Illuminate\Console\Application::starting`
 * in this package's service provider so registration order is deterministic.
 */
#[AsCommand(
    name: 'clickhouse:migrate',
    description: 'Run the Clickhouse database migrations',
)]
class MigrateCommand extends Command
{
    use ConfirmableTrait;

    protected $signature = 'clickhouse:migrate
                {--database= : The Clickhouse database name to migrate (not a Laravel connection name)}
                {--force : Force the operation to run when in production}
                {--path=* : The path(s) to the migration files to be executed}
                {--realpath : Indicate any provided migration file paths are pre-resolved absolute paths}
                {--schema-path= : The path to a schema dump file}
                {--step=0 : Number of migrations to run (0 = all)}
                {--graceful : Return a successful exit code even if an error occurs}';

    private ?SchemaInspector $inspector = null;

    public function __construct(
        protected readonly Migrator $migrator,
        protected readonly Dispatcher $dispatcher,
        protected readonly Filesystem $filesystem,
        protected readonly AppConfigRepositoryInterface $appConfig,
    ) {
        parent::__construct();
    }

    public function handle(): int
    {
        $this->inspector = null;

        if (! $this->confirmToProceed()) {
            return self::FAILURE;
        }

        try {
            $this->runMigrations();
        } catch (Throwable $e) {
            if ($this->option('graceful')) {
                $this->components->warn($e->getMessage());

                return self::SUCCESS;
            }

            throw $e;
        }

        return self::SUCCESS;
    }

    protected function runMigrations(): void
    {
        $this->prepareDatabase();

        foreach ($this->getMigrationPaths() as $path) {
            $this->migrator->runUp($path, $this->getOutput(), $this->getStep());
        }
    }

    protected function prepareDatabase(): void
    {
        $this->migrator->ensureTableExists();

        if (! $this->hasRunAnyMigrations()) {
            $this->loadSchemaState();
        }
    }

    protected function hasRunAnyMigrations(): bool
    {
        $inspector = $this->inspector();

        return $inspector->countRows($inspector->migrationsTable()) > 0;
    }

    /**
     * Replay a previously-dumped schema file. Drops the migrations log table
     * before replaying so the dump's `CREATE TABLE migrations` does not collide.
     *
     * NOTE: Clickhouse has no transactional DDL — if a statement fails partway
     * through the replay, partial state will remain. Re-running the command
     * after fixing the dump is the recovery path.
     */
    protected function loadSchemaState(): void
    {
        $path = $this->schemaPath();

        if (! is_string($path) || ! is_file($path)) {
            return;
        }

        $inspector = $this->inspector();

        $this->components->info('Loading stored Clickhouse schema.');

        $this->components->task($path, function () use ($path, $inspector): bool {
            $inspector->dropTable($inspector->migrationsTable());

            $client = $this->client();
            foreach ($this->splitStatements((string) $this->filesystem->get($path)) as $statement) {
                $client->write($statement);
            }

            return true;
        });

        $this->newLine();

        $this->dispatcher->dispatch(new ClickhouseSchemaLoaded(
            $inspector->database(),
            $path,
        ));
    }

    protected function schemaPath(): ?string
    {
        $custom = $this->option('schema-path');
        if (is_string($custom) && $custom !== '') {
            return $custom;
        }

        $sqlPath = database_path('clickhouse-schema/schema.sql');
        if (file_exists($sqlPath)) {
            return $sqlPath;
        }

        return null;
    }

    /** @return list<string> */
    protected function getMigrationPaths(): array
    {
        $paths = (array) $this->option('path');
        if (count($paths) > 0) {
            $realpath = (bool) $this->option('realpath');

            return array_values(array_map(
                static fn ($p): string => $realpath ? (string) $p : base_path((string) $p),
                $paths,
            ));
        }

        return [(string) $this->appConfig->get('clickhouse.migrations.path')];
    }

    protected function getStep(): int
    {
        return (int) $this->option('step');
    }

    /**
     * Split a SQL dump into individual statements.
     *
     * Contract: this only knows how to parse dumps written by
     * `clickhouse:schema-dump`, which separates statements with `;\n\n`. Hand-
     * written dumps that don't follow that separator may be split incorrectly.
     *
     * @return list<string>
     */
    protected function splitStatements(string $sql): array
    {
        $parts = preg_split('/;\s*\n\s*\n/', $sql);
        if ($parts === false) {
            $parts = [$sql];
        }

        $statements = [];
        foreach ($parts as $part) {
            $trimmed = trim((string) $part);
            $trimmed = rtrim($trimmed, ';');
            $trimmed = trim($trimmed);
            if ($trimmed !== '') {
                $statements[] = $trimmed;
            }
        }

        return $statements;
    }

    protected function inspector(): SchemaInspector
    {
        if ($this->inspector !== null) {
            return $this->inspector;
        }

        $database = $this->option('database');

        return $this->inspector = SchemaInspector::forDefault(
            is_string($database) && $database !== '' ? $database : null,
        );
    }

    protected function client(): ClickhouseClient
    {
        return $this->laravel->make(ClickhouseClient::class);
    }
}
