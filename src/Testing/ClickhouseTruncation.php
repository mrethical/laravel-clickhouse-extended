<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\Testing;

use ClickHouseDB\Client as ClickhouseClient;
use Illuminate\Contracts\Console\Kernel;
use Illuminate\Support\Facades\ParallelTesting;
use Mrethical\LaravelClickhouseExtended\Support\DatabaseBootstrap;
use Mrethical\LaravelClickhouseExtended\Support\SchemaInspector;

/**
 * Truncates Clickhouse tables between tests, mirroring Laravel's
 * Illuminate\Foundation\Testing\DatabaseTruncation.
 *
 * On every invocation:
 *   - If running under `--parallel`, swap the configured database to the
 *     worker-suffixed name (e.g. `default_2`) and `CREATE DATABASE IF NOT
 *     EXISTS` it. This must run per test because Laravel rebuilds the
 *     container between tests, which re-reads the connection config from env.
 *
 * On the first invocation per test process:
 *   1. Run `clickhouse:migrate-fresh --force` to bring the schema up to date.
 *   2. Issue `SYSTEM STOP VIEW` on every materialized view (opt-out via
 *      `protected bool $stopRefreshableViews = false;`) so refreshable MVs
 *      don't overwrite test data on their refresh schedule.
 *
 * On subsequent invocations: truncate user tables (the migrations log is
 * preserved).
 *
 * Optional opt-in properties on the test class:
 *
 *     protected array  $tablesToTruncate    = ['users', 'events'];   // whitelist
 *     protected array  $exceptTables        = ['audit_log'];         // blacklist
 *     protected ?string $clickhouseDatabase = 'analytics_test';      // override DB
 *     protected bool   $stopRefreshableViews = false;                // disable MV stop step
 */
trait ClickhouseTruncation
{
    public function setUpClickhouseTruncation(): void
    {
        $this->truncateClickhouseDatabase();
    }

    protected function truncateClickhouseDatabase(): void
    {
        $this->beforeTruncatingClickhouseDatabase();

        // Must run per test: Laravel rebuilds the container between tests,
        // which re-reads `clickhouse.connection.options.database` from env
        // back to its base value, undoing any previous worker-token swap.
        $this->ensureClickhouseDatabaseForCurrentTestToken();

        if (! ClickhouseTruncationState::$migrated) {
            $this->artisan('clickhouse:migrate-fresh', $this->clickhouseMigrateFreshUsing());

            $this->app[Kernel::class]->setArtisan(null);

            if ($this->shouldStopRefreshableViews()) {
                $this->stopRefreshableClickhouseViews();
            }

            ClickhouseTruncationState::$migrated = true;

            $this->afterTruncatingClickhouseDatabase();

            return;
        }

        $this->truncateClickhouseTables();

        $this->afterTruncatingClickhouseDatabase();
    }

    protected function truncateClickhouseTables(): void
    {
        $inspector = SchemaInspector::forDefault($this->clickhouseDatabase());

        $tables = $inspector->tables();
        $whitelist = $this->clickhouseTablesToTruncate();

        if ($whitelist !== null) {
            $tables = array_values(array_filter(
                $tables,
                static fn (string $name): bool => in_array($name, $whitelist, true),
            ));
        } else {
            $skip = $this->clickhouseExceptTables($inspector);
            $tables = array_values(array_filter(
                $tables,
                static fn (string $name): bool => ! in_array($name, $skip, true),
            ));
        }

        foreach ($tables as $table) {
            $inspector->truncateTable($table);
        }
    }

    /**
     * If running under `php artisan test --parallel`, point this worker at a
     * dedicated database (creating it if needed) so workers don't stomp each
     * other.
     */
    protected function ensureClickhouseDatabaseForCurrentTestToken(): void
    {
        $token = $this->clickhouseTestToken();

        if ($token === null || $token === '' || $token === false) {
            return;
        }

        $base = (string) $this->app['config']->get('clickhouse.connection.options.database', 'default');
        $database = $base.'_'.$token;

        $this->app['config']->set('clickhouse.connection.options.database', $database);
        $this->app->forgetInstance(ClickhouseClient::class);

        DatabaseBootstrap::ensureDatabase(
            $database,
            (array) $this->app['config']->get('clickhouse.connection', []),
        );
    }

    protected function stopRefreshableClickhouseViews(): void
    {
        $inspector = SchemaInspector::forDefault($this->clickhouseDatabase());

        foreach ($inspector->materializedViews() as $view) {
            $inspector->stopView($view);
        }
    }

    /**
     * The current parallel-testing worker token, or false/null when running serial.
     *
     * @return int|string|false|null
     */
    protected function clickhouseTestToken()
    {
        return ParallelTesting::token();
    }

    protected function shouldStopRefreshableViews(): bool
    {
        return property_exists($this, 'stopRefreshableViews')
            ? (bool) $this->stopRefreshableViews
            : true;
    }

    protected function clickhouseDatabase(): ?string
    {
        return property_exists($this, 'clickhouseDatabase') && is_string($this->clickhouseDatabase) && $this->clickhouseDatabase !== ''
            ? $this->clickhouseDatabase
            : null;
    }

    /** @return array<int, string>|null */
    protected function clickhouseTablesToTruncate(): ?array
    {
        return property_exists($this, 'tablesToTruncate') && is_array($this->tablesToTruncate)
            ? $this->tablesToTruncate
            : null;
    }

    /** @return array<int, string> */
    protected function clickhouseExceptTables(SchemaInspector $inspector): array
    {
        $migrationsTable = $inspector->migrationsTable();

        return property_exists($this, 'exceptTables') && is_array($this->exceptTables)
            ? array_values(array_unique(array_merge($this->exceptTables, [$migrationsTable])))
            : [$migrationsTable];
    }

    /** @return array<string, mixed> */
    protected function clickhouseMigrateFreshUsing(): array
    {
        return ['--force' => true];
    }

    protected function beforeTruncatingClickhouseDatabase(): void
    {
        //
    }

    protected function afterTruncatingClickhouseDatabase(): void
    {
        //
    }
}
