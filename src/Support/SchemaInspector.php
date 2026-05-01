<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\Support;

use ClickHouseDB\Client as ClickhouseClient;
use Illuminate\Container\Container;

final class SchemaInspector
{
    private ?string $cachedMigrationsTable = null;

    public function __construct(
        private readonly ClickhouseClient $client,
        private readonly string $database,
    ) {}

    /**
     * Resolve a SchemaInspector through the container so the binding
     * (registered by the package's service provider) can be overridden in tests.
     */
    public static function forDefault(?string $database = null): self
    {
        $container = Container::getInstance();
        $params = $database !== null && $database !== '' ? ['database' => $database] : [];

        return $container->make(self::class, $params);
    }

    public function database(): string
    {
        return $this->database;
    }

    public function migrationsTable(): string
    {
        return $this->cachedMigrationsTable ??= (string) Container::getInstance()
            ->make('config')
            ->get('clickhouse.migrations.table', 'migrations');
    }

    public function tableExists(string $table): bool
    {
        $sql = sprintf('EXISTS TABLE %s.%s', self::quoteIdent($this->database), self::quoteIdent($table));
        $rows = $this->client->select($sql)->rows();

        return ((int) ($rows[0]['result'] ?? 0)) === 1;
    }

    /**
     * Every table-like entity in the configured database (regular tables, views, materialized views).
     *
     * @return list<string>
     */
    public function allTables(): array
    {
        $sql = sprintf(
            "SELECT name FROM system.tables WHERE database = '%s' ORDER BY name",
            self::escapeString($this->database),
        );

        return array_map(
            static fn (array $row): string => (string) $row['name'],
            $this->client->select($sql)->rows(),
        );
    }

    /**
     * Real tables in the configured database (excludes views and materialized views).
     *
     * @return list<string>
     */
    public function tables(): array
    {
        $sql = sprintf(
            "SELECT name FROM system.tables WHERE database = '%s' AND engine NOT LIKE '%%View' ORDER BY name",
            self::escapeString($this->database),
        );

        return array_map(
            static fn (array $row): string => (string) $row['name'],
            $this->client->select($sql)->rows(),
        );
    }

    /**
     * Views (regular and materialized) in the configured database.
     *
     * @return list<string>
     */
    public function views(): array
    {
        $sql = sprintf(
            "SELECT name FROM system.tables WHERE database = '%s' AND engine LIKE '%%View' ORDER BY name",
            self::escapeString($this->database),
        );

        return array_map(
            static fn (array $row): string => (string) $row['name'],
            $this->client->select($sql)->rows(),
        );
    }

    /**
     * Materialized views in the configured database.
     *
     * @return list<string>
     */
    public function materializedViews(): array
    {
        $sql = sprintf(
            "SELECT name FROM system.tables WHERE database = '%s' AND engine = 'MaterializedView' ORDER BY name",
            self::escapeString($this->database),
        );

        return array_map(
            static fn (array $row): string => (string) $row['name'],
            $this->client->select($sql)->rows(),
        );
    }

    /** @return list<string> */
    public function dictionaries(): array
    {
        $sql = sprintf(
            "SELECT name FROM system.dictionaries WHERE database = '%s' ORDER BY name",
            self::escapeString($this->database),
        );

        return array_map(
            static fn (array $row): string => (string) $row['name'],
            $this->client->select($sql)->rows(),
        );
    }

    public function showCreate(string $table): string
    {
        $sql = sprintf('SHOW CREATE TABLE %s.%s', self::quoteIdent($this->database), self::quoteIdent($table));
        $rows = $this->client->select($sql)->rows();

        return (string) ($rows[0]['statement'] ?? '');
    }

    public function countRows(string $table): int
    {
        $sql = sprintf('SELECT count() AS n FROM %s.%s', self::quoteIdent($this->database), self::quoteIdent($table));
        $rows = $this->client->select($sql)->rows();

        return (int) ($rows[0]['n'] ?? 0);
    }

    /** @return list<array<string, mixed>> */
    public function selectAll(string $table, string $orderBy = ''): array
    {
        $sql = sprintf('SELECT * FROM %s.%s', self::quoteIdent($this->database), self::quoteIdent($table));
        if ($orderBy !== '') {
            $sql .= ' ORDER BY '.$orderBy;
        }

        return $this->client->select($sql)->rows();
    }

    public function dropTable(string $table): void
    {
        $sql = sprintf('DROP TABLE IF EXISTS %s.%s', self::quoteIdent($this->database), self::quoteIdent($table));
        $this->client->write($sql);
    }

    public function dropDictionary(string $dictionary): void
    {
        $sql = sprintf('DROP DICTIONARY IF EXISTS %s.%s', self::quoteIdent($this->database), self::quoteIdent($dictionary));
        $this->client->write($sql);
    }

    public function truncateTable(string $table): void
    {
        $sql = sprintf('TRUNCATE TABLE IF EXISTS %s.%s', self::quoteIdent($this->database), self::quoteIdent($table));
        $this->client->write($sql);
    }

    public function stopView(string $view): void
    {
        $sql = sprintf('SYSTEM STOP VIEW %s.%s', self::quoteIdent($this->database), self::quoteIdent($view));
        $this->client->write($sql);
    }

    /** Wrap a Clickhouse identifier in backticks, escaping any embedded backticks. */
    public static function quoteIdent(string $identifier): string
    {
        return '`'.str_replace('`', '``', $identifier).'`';
    }

    /** Escape a value for use inside a single-quoted Clickhouse string literal. */
    public static function escapeString(string $value): string
    {
        return str_replace(['\\', "'"], ['\\\\', "\\'"], $value);
    }
}
