<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\ConsoleCommand;

use Illuminate\Console\Command;
use Illuminate\Console\Prohibitable;
use Illuminate\Contracts\Config\Repository as AppConfigRepositoryInterface;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Filesystem\Filesystem;
use Mrethical\LaravelClickhouseExtended\Events\ClickhouseMigrationsPruned;
use Mrethical\LaravelClickhouseExtended\Events\ClickhouseSchemaDumped;
use Mrethical\LaravelClickhouseExtended\Support\SchemaInspector;
use Symfony\Component\Console\Attribute\AsCommand;

/**
 * Mirrors illuminate/database/Console/DumpCommand (Laravel 12.x), adapted for
 * Clickhouse. Dumps `SHOW CREATE TABLE` statements plus an `INSERT` for the
 * migration log rows so the dump is a complete checkpoint that
 * `clickhouse:migrate` can replay via `--schema-path`.
 */
#[AsCommand(
    name: 'clickhouse:schema-dump',
    description: 'Dump the given Clickhouse database schema',
)]
class SchemaDumpCommand extends Command
{
    use Prohibitable;

    protected $signature = 'clickhouse:schema-dump
                {--database= : The Clickhouse database name to dump (not a Laravel connection name)}
                {--path= : The path where the schema dump file should be stored}
                {--prune : Delete all existing migration files}';

    public function __construct(
        protected readonly Filesystem $filesystem,
        protected readonly AppConfigRepositoryInterface $appConfig,
        protected readonly Dispatcher $dispatcher,
    ) {
        parent::__construct();
    }

    public function handle(): int
    {
        if ($this->isProhibited()) {
            return self::FAILURE;
        }

        $database = $this->option('database');
        $inspector = SchemaInspector::forDefault(is_string($database) && $database !== '' ? $database : null);

        $statements = [];
        $migrationsTable = $inspector->migrationsTable();
        // Skip auto-managed `.inner.*` / `.inner_id.*` tables — Clickhouse
        // creates and drops them automatically as the storage for an MV
        // without a `TO` target. Including them in the dump would conflict
        // with the MV's own create on replay.
        $tables = array_values(array_filter(
            $inspector->tables(),
            static fn (string $t): bool => ! str_starts_with($t, '.inner'),
        ));
        $views = $inspector->views();

        // Tables first — a materialized view's `FROM source_table` must
        // resolve on replay.
        foreach ($tables as $table) {
            $statements[] = $this->stripDatabasePrefix($inspector->database(), $inspector->showCreate($table)).';';
        }

        // Views are topo-sorted so an MV that reads from another MV
        // (chain or fan-in) is emitted after its dependencies.
        $viewDdls = [];
        foreach ($views as $view) {
            $viewDdls[$view] = $inspector->showCreate($view);
        }
        foreach ($this->topoSortViews($views, $viewDdls) as $view) {
            $statements[] = $this->stripDatabasePrefix($inspector->database(), $viewDdls[$view]).';';
        }

        if (in_array($migrationsTable, $tables, true)) {
            $insert = $this->buildMigrationsInsert($inspector, $migrationsTable);
            if ($insert !== null) {
                $statements[] = $insert;
            }
        }

        $path = $this->resolvePath();
        $this->filesystem->ensureDirectoryExists(dirname($path));
        $this->filesystem->put($path, implode("\n\n", $statements).(count($statements) > 0 ? "\n" : ''));

        $this->dispatcher->dispatch(new ClickhouseSchemaDumped($inspector->database(), $path));

        $info = 'Database schema dumped';

        if ($this->option('prune')) {
            $migrationsPath = $this->pruneMigrations();

            if ($migrationsPath !== null) {
                $info .= ' and pruned';
                $this->dispatcher->dispatch(new ClickhouseMigrationsPruned($inspector->database(), $migrationsPath));
            }
        }

        $this->components->info($info.' successfully.');

        return self::SUCCESS;
    }

    protected function resolvePath(): string
    {
        $path = $this->option('path');
        if (is_string($path) && $path !== '') {
            return $path;
        }

        return database_path('clickhouse-schema/schema.sql');
    }

    protected function pruneMigrations(): ?string
    {
        $migrationsPath = (string) $this->appConfig->get('clickhouse.migrations.path');

        if ($migrationsPath === '' || ! $this->filesystem->isDirectory($migrationsPath)) {
            return null;
        }

        $this->filesystem->deleteDirectory($migrationsPath, preserve: true);

        return $migrationsPath;
    }

    protected function buildMigrationsInsert(SchemaInspector $inspector, string $migrationsTable): ?string
    {
        $rows = $inspector->selectAll($migrationsTable, 'migration');
        if (count($rows) === 0) {
            return null;
        }

        $values = [];
        foreach ($rows as $row) {
            $migration = SchemaInspector::escapeString((string) ($row['migration'] ?? ''));
            $batch = (int) ($row['batch'] ?? 0);
            $values[] = sprintf("('%s', %d, '1970-01-01 00:00:00')", $migration, $batch);
        }

        return sprintf(
            'INSERT INTO %s (migration, batch, applied_at) VALUES %s;',
            SchemaInspector::quoteIdent($migrationsTable),
            implode(', ', $values),
        );
    }

    /**
     * Strip occurrences of `<database>.` (with or without backticks) so the dump
     * is portable across databases — when replayed by `clickhouse:migrate`, the
     * connection's current database resolves the unqualified names. Other
     * database qualifiers (e.g. references to a shared `meta` DB) are preserved.
     */
    protected function stripDatabasePrefix(string $database, string $sql): string
    {
        $quoted = preg_quote($database, '/');
        $pattern = '/(?<![A-Za-z0-9_`])(?:`'.$quoted.'`|'.$quoted.')\./';

        return (string) preg_replace($pattern, '', $sql);
    }

    /**
     * Order views so each is emitted after every other view it references.
     * Uses Kahn's algorithm with alphabetical tie-breaking. On a cycle (which
     * Clickhouse should not allow at create time), the remaining views are
     * appended alphabetically and replay surfaces the issue.
     *
     * Detection is regex-based: a view is treated as a dep if its name appears
     * in another view's DDL as a standalone identifier. Column or alias names
     * that collide with a view name produce a spurious edge — the practical
     * cost is only a slightly different (but still valid) emission order.
     *
     * @param  list<string>  $views
     * @param  array<string, string>  $ddls  view name => raw CREATE statement
     * @return list<string>
     */
    protected function topoSortViews(array $views, array $ddls): array
    {
        $adj = [];
        $inDegree = [];
        foreach ($views as $v) {
            $adj[$v] = [];
            $inDegree[$v] = 0;
        }

        foreach ($views as $dependent) {
            $ddl = $ddls[$dependent] ?? '';
            foreach ($views as $dep) {
                if ($dep === $dependent) {
                    continue;
                }
                if ($this->ddlReferencesName($ddl, $dep)) {
                    $adj[$dep][] = $dependent;
                    $inDegree[$dependent]++;
                }
            }
        }

        $ready = [];
        foreach ($views as $v) {
            if ($inDegree[$v] === 0) {
                $ready[] = $v;
            }
        }
        sort($ready);

        $sorted = [];
        while (count($ready) > 0) {
            $v = array_shift($ready);
            $sorted[] = $v;
            foreach ($adj[$v] as $next) {
                $inDegree[$next]--;
                if ($inDegree[$next] === 0) {
                    $ready[] = $next;
                    sort($ready);
                }
            }
        }

        if (count($sorted) < count($views)) {
            $remaining = array_values(array_diff($views, $sorted));
            sort($remaining);
            $sorted = array_merge($sorted, $remaining);
        }

        return $sorted;
    }

    protected function ddlReferencesName(string $ddl, string $name): bool
    {
        $quoted = preg_quote($name, '/');
        $pattern = '/(?<![A-Za-z0-9_`])(?:`'.$quoted.'`|'.$quoted.')(?![A-Za-z0-9_])/';

        return preg_match($pattern, $ddl) === 1;
    }
}
