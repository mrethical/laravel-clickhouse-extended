<?php

declare(strict_types=1);

use ClickHouseDB\Client as ClickhouseClient;
use ClickHouseDB\Statement;
use Illuminate\Support\Facades\Event;
use Mrethical\LaravelClickhouseExtended\Events\ClickhouseSchemaLoaded;

beforeEach(function () {
    $this->migrationsDir = sys_get_temp_dir().'/lce-migrate-'.uniqid();
    $this->schemaDir = sys_get_temp_dir().'/lce-schema-'.uniqid();
    mkdir($this->migrationsDir, 0o777, true);
    mkdir($this->schemaDir, 0o777, true);
    config()->set('clickhouse.migrations.path', $this->migrationsDir);
});

afterEach(function () {
    foreach ([$this->migrationsDir, $this->schemaDir] as $dir) {
        if (is_dir($dir)) {
            foreach (glob($dir.'/*') ?: [] as $f) {
                @unlink($f);
            }
            @rmdir($dir);
        }
    }
});

function stubAllMigrationsStatement(array $rows = []): Statement
{
    $stmt = Mockery::mock(Statement::class);
    $stmt->shouldReceive('rows')->andReturn($rows);

    return $stmt;
}

it('runs ensureTableExists and runUp with the configured migrations path', function () {
    $existsStmt = Mockery::mock(Statement::class);
    $existsStmt->shouldReceive('fetchOne')->with('result')->once()->andReturn(true);

    $countStmt = Mockery::mock(Statement::class);
    $countStmt->shouldReceive('rows')->once()->andReturn([['n' => 5]]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/EXISTS TABLE/'), Mockery::any())
        ->andReturn($existsStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/SELECT count\\(\\) AS n FROM `default`\\.`migrations`/'))
        ->andReturn($countStmt);
    $client->shouldReceive('select')
        ->with(Mockery::pattern('/SELECT migration/'), Mockery::any())
        ->andReturn(stubAllMigrationsStatement());
    $this->app->instance(ClickhouseClient::class, $client);

    $this->artisan('clickhouse:migrate', ['--force' => true])->assertSuccessful();
});

it('loads the schema dump when migrations log is empty and a dump exists', function () {
    $schemaPath = $this->schemaDir.'/schema.sql';
    file_put_contents($schemaPath, "CREATE TABLE default.users (id UInt64) ENGINE = MergeTree ORDER BY id;\n\nINSERT INTO `default`.`migrations` (migration, batch, applied_at) VALUES ('2024_01_01_000000_create_users', 1, '2024-01-15 10:00:00');\n");

    $existsStmt = Mockery::mock(Statement::class);
    $existsStmt->shouldReceive('fetchOne')->with('result')->once()->andReturn(true);

    $countStmt = Mockery::mock(Statement::class);
    $countStmt->shouldReceive('rows')->once()->andReturn([['n' => 0]]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/EXISTS TABLE/'), Mockery::any())
        ->andReturn($existsStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/SELECT count\\(\\) AS n/'))
        ->andReturn($countStmt);
    $client->shouldReceive('write')->once()->with('DROP TABLE IF EXISTS `default`.`migrations`');
    $client->shouldReceive('write')->once()->with('CREATE TABLE default.users (id UInt64) ENGINE = MergeTree ORDER BY id');
    $client->shouldReceive('write')->once()->with("INSERT INTO `default`.`migrations` (migration, batch, applied_at) VALUES ('2024_01_01_000000_create_users', 1, '2024-01-15 10:00:00')");
    $client->shouldReceive('select')
        ->with(Mockery::pattern('/SELECT migration/'), Mockery::any())
        ->andReturn(stubAllMigrationsStatement());
    $this->app->instance(ClickhouseClient::class, $client);

    Event::fake([ClickhouseSchemaLoaded::class]);

    $this->artisan('clickhouse:migrate', [
        '--force' => true,
        '--schema-path' => $schemaPath,
    ])->assertSuccessful();

    Event::assertDispatched(
        ClickhouseSchemaLoaded::class,
        fn (ClickhouseSchemaLoaded $e) => $e->path === $schemaPath && $e->database === 'default',
    );
});

it('skips schema loading when migrations log already has rows', function () {
    $schemaPath = $this->schemaDir.'/schema.sql';
    file_put_contents($schemaPath, "CREATE TABLE default.users (id UInt64) ENGINE = MergeTree ORDER BY id;\n");

    $existsStmt = Mockery::mock(Statement::class);
    $existsStmt->shouldReceive('fetchOne')->with('result')->once()->andReturn(true);

    $countStmt = Mockery::mock(Statement::class);
    $countStmt->shouldReceive('rows')->once()->andReturn([['n' => 3]]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/EXISTS TABLE/'), Mockery::any())
        ->andReturn($existsStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/SELECT count\\(\\) AS n/'))
        ->andReturn($countStmt);
    $client->shouldNotReceive('write');
    $client->shouldReceive('select')
        ->with(Mockery::pattern('/SELECT migration/'), Mockery::any())
        ->andReturn(stubAllMigrationsStatement());
    $this->app->instance(ClickhouseClient::class, $client);

    $this->artisan('clickhouse:migrate', [
        '--force' => true,
        '--schema-path' => $schemaPath,
    ])->assertSuccessful();
});

it('returns success when --graceful is set and the prepare step throws', function () {
    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/EXISTS TABLE/'), Mockery::any())
        ->andThrow(new RuntimeException('migration table is corrupted'));
    $this->app->instance(ClickhouseClient::class, $client);

    $this->artisan('clickhouse:migrate', [
        '--force' => true,
        '--graceful' => true,
    ])->assertSuccessful();
});

it('rethrows when --graceful is not set', function () {
    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/EXISTS TABLE/'), Mockery::any())
        ->andThrow(new RuntimeException('boom'));
    $this->app->instance(ClickhouseClient::class, $client);

    expect(fn () => $this->artisan('clickhouse:migrate', ['--force' => true])->run())
        ->toThrow(RuntimeException::class, 'boom');
});
