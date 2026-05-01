<?php

declare(strict_types=1);

use ClickHouseDB\Client as ClickhouseClient;
use ClickHouseDB\Statement;
use Illuminate\Support\Facades\Event;
use Mrethical\LaravelClickhouseExtended\Events\ClickhouseMigrationsPruned;
use Mrethical\LaravelClickhouseExtended\Events\ClickhouseSchemaDumped;

beforeEach(function () {
    $this->dumpDir = sys_get_temp_dir().'/lce-dump-'.uniqid();
});

afterEach(function () {
    if (is_dir($this->dumpDir)) {
        foreach (glob($this->dumpDir.'/*') ?: [] as $f) {
            if (is_dir($f)) {
                foreach (glob($f.'/*') ?: [] as $f2) {
                    @unlink($f2);
                }
                @rmdir($f);
            } else {
                @unlink($f);
            }
        }
        @rmdir($this->dumpDir);
    }
});

it('writes a SHOW CREATE statement per table to the schema file and dispatches SchemaDumped', function () {
    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'users'],
        ['name' => 'events'],
    ]);

    $viewsStmt = Mockery::mock(Statement::class);
    $viewsStmt->shouldReceive('rows')->once()->andReturn([]);

    $usersDdl = Mockery::mock(Statement::class);
    $usersDdl->shouldReceive('rows')->once()->andReturn([
        ['statement' => 'CREATE TABLE default.users (id UInt64) ENGINE = MergeTree ORDER BY id'],
    ]);

    $eventsDdl = Mockery::mock(Statement::class);
    $eventsDdl->shouldReceive('rows')->once()->andReturn([
        ['statement' => 'CREATE TABLE default.events (id UInt64) ENGINE = MergeTree ORDER BY id'],
    ]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine NOT LIKE/'))
        ->andReturn($tablesStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine LIKE/'))
        ->andReturn($viewsStmt);
    $client->shouldReceive('select')->once()
        ->with('SHOW CREATE TABLE `default`.`users`')
        ->andReturn($usersDdl);
    $client->shouldReceive('select')->once()
        ->with('SHOW CREATE TABLE `default`.`events`')
        ->andReturn($eventsDdl);

    $this->app->instance(ClickhouseClient::class, $client);

    Event::fake([ClickhouseSchemaDumped::class]);

    $path = $this->dumpDir.'/schema.sql';
    $this->artisan('clickhouse:schema-dump', ['--path' => $path])->assertSuccessful();

    expect(file_get_contents($path))
        ->toContain('CREATE TABLE users')
        ->toContain('CREATE TABLE events')
        ->not->toContain('default.users')
        ->not->toContain('default.events');

    Event::assertDispatched(
        ClickhouseSchemaDumped::class,
        fn (ClickhouseSchemaDumped $e) => $e->path === $path && $e->database === 'default',
    );
});

it('appends an INSERT for migrations log rows when the table is present', function () {
    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'migrations'],
    ]);

    $viewsStmt = Mockery::mock(Statement::class);
    $viewsStmt->shouldReceive('rows')->once()->andReturn([]);

    $migrationsDdl = Mockery::mock(Statement::class);
    $migrationsDdl->shouldReceive('rows')->once()->andReturn([
        ['statement' => 'CREATE TABLE default.migrations (migration String, batch UInt32, applied_at DateTime DEFAULT NOW()) ENGINE = ReplacingMergeTree() ORDER BY migration'],
    ]);

    // Source rows in arbitrary order — dump must sort INSERT VALUES by
    // migration name so dumps are stable across regenerations.
    $migrationsRows = Mockery::mock(Statement::class);
    $migrationsRows->shouldReceive('rows')->once()->andReturn([
        ['migration' => '2024_01_01_000000_create_users', 'batch' => 1, 'applied_at' => '2024-01-15 10:00:00'],
        ['migration' => '2024_02_01_000000_create_events', 'batch' => 2, 'applied_at' => '2024-02-15 11:30:00'],
    ]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine NOT LIKE/'))
        ->andReturn($tablesStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine LIKE/'))
        ->andReturn($viewsStmt);
    $client->shouldReceive('select')->once()
        ->with('SHOW CREATE TABLE `default`.`migrations`')
        ->andReturn($migrationsDdl);
    $client->shouldReceive('select')->once()
        ->with('SELECT * FROM `default`.`migrations` ORDER BY migration')
        ->andReturn($migrationsRows);

    $this->app->instance(ClickhouseClient::class, $client);

    $path = $this->dumpDir.'/schema.sql';
    $this->artisan('clickhouse:schema-dump', ['--path' => $path])->assertSuccessful();

    $contents = (string) file_get_contents($path);
    expect($contents)
        ->toContain('CREATE TABLE migrations')
        ->toContain('INSERT INTO `migrations` (migration, batch, applied_at) VALUES')
        ->toContain("('2024_01_01_000000_create_users', 1, '1970-01-01 00:00:00')")
        ->toContain("('2024_02_01_000000_create_events', 2, '1970-01-01 00:00:00')")
        ->not->toContain('default.migrations')
        ->not->toContain('`default`.`migrations`');

    // Verify migration name ordering inside the INSERT.
    expect(strpos($contents, 'create_users'))
        ->toBeLessThan(strpos($contents, 'create_events'));
});

it('only strips the source database prefix, preserving references to other databases', function () {
    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([]);

    $viewsStmt = Mockery::mock(Statement::class);
    $viewsStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'events_mv'],
    ]);

    $mvDdl = Mockery::mock(Statement::class);
    $mvDdl->shouldReceive('rows')->once()->andReturn([
        ['statement' => 'CREATE MATERIALIZED VIEW `default`.`events_mv` TO default.events_target AS SELECT * FROM meta.dictionary_source'],
    ]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine NOT LIKE/'))
        ->andReturn($tablesStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine LIKE/'))
        ->andReturn($viewsStmt);
    $client->shouldReceive('select')->once()
        ->with('SHOW CREATE TABLE `default`.`events_mv`')
        ->andReturn($mvDdl);

    $this->app->instance(ClickhouseClient::class, $client);

    $path = $this->dumpDir.'/schema.sql';
    $this->artisan('clickhouse:schema-dump', ['--path' => $path])->assertSuccessful();

    expect(file_get_contents($path))
        ->toContain('CREATE MATERIALIZED VIEW `events_mv` TO events_target AS SELECT * FROM meta.dictionary_source')
        ->not->toContain('default.events_mv')
        ->not->toContain('`default`.`events_mv`')
        ->not->toContain('default.events_target');
});

it('prunes migration files and dispatches MigrationsPruned when --prune is set', function () {
    $migrationsDir = $this->dumpDir.'/migrations';
    mkdir($migrationsDir, 0o777, true);
    file_put_contents($migrationsDir.'/2024_01_01_000000_create_users.php', '<?php');
    file_put_contents($migrationsDir.'/2024_02_01_000000_create_events.php', '<?php');

    config()->set('clickhouse.migrations.path', $migrationsDir);

    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([]);

    $viewsStmt = Mockery::mock(Statement::class);
    $viewsStmt->shouldReceive('rows')->once()->andReturn([]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine NOT LIKE/'))
        ->andReturn($tablesStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine LIKE/'))
        ->andReturn($viewsStmt);

    $this->app->instance(ClickhouseClient::class, $client);

    Event::fake([ClickhouseMigrationsPruned::class]);

    $this->artisan('clickhouse:schema-dump', [
        '--path' => $this->dumpDir.'/schema.sql',
        '--prune' => true,
    ])->assertSuccessful();

    expect(glob($migrationsDir.'/*'))->toBe([]);

    Event::assertDispatched(
        ClickhouseMigrationsPruned::class,
        fn (ClickhouseMigrationsPruned $e) => $e->path === $migrationsDir,
    );
});

it('emits real tables before views so MV references resolve on replay', function () {
    // `aaa_mv` sorts before `events` alphabetically — without table/view
    // separation it would be dumped first and its `FROM events` would fail.
    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'events'],
    ]);

    $viewsStmt = Mockery::mock(Statement::class);
    $viewsStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'aaa_mv'],
    ]);

    $eventsDdl = Mockery::mock(Statement::class);
    $eventsDdl->shouldReceive('rows')->once()->andReturn([
        ['statement' => 'CREATE TABLE default.events (id UInt64) ENGINE = MergeTree ORDER BY id'],
    ]);

    $mvDdl = Mockery::mock(Statement::class);
    $mvDdl->shouldReceive('rows')->once()->andReturn([
        ['statement' => 'CREATE MATERIALIZED VIEW default.aaa_mv ENGINE = MergeTree ORDER BY id AS SELECT * FROM default.events'],
    ]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine NOT LIKE/'))
        ->andReturn($tablesStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine LIKE/'))
        ->andReturn($viewsStmt);
    $client->shouldReceive('select')->once()
        ->with('SHOW CREATE TABLE `default`.`events`')
        ->andReturn($eventsDdl);
    $client->shouldReceive('select')->once()
        ->with('SHOW CREATE TABLE `default`.`aaa_mv`')
        ->andReturn($mvDdl);

    $this->app->instance(ClickhouseClient::class, $client);

    $path = $this->dumpDir.'/schema.sql';
    $this->artisan('clickhouse:schema-dump', ['--path' => $path])->assertSuccessful();

    $contents = (string) file_get_contents($path);
    expect(strpos($contents, 'CREATE TABLE events'))->toBeLessThan(strpos($contents, 'CREATE MATERIALIZED VIEW aaa_mv'));
});

it('skips Clickhouse-managed .inner and .inner_id tables', function () {
    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => '.inner.events_mv'],
        ['name' => '.inner_id.0123abcd-ef01-2345-6789-abcdef012345'],
        ['name' => 'events'],
    ]);

    $viewsStmt = Mockery::mock(Statement::class);
    $viewsStmt->shouldReceive('rows')->once()->andReturn([]);

    $eventsDdl = Mockery::mock(Statement::class);
    $eventsDdl->shouldReceive('rows')->once()->andReturn([
        ['statement' => 'CREATE TABLE default.events (id UInt64) ENGINE = MergeTree ORDER BY id'],
    ]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine NOT LIKE/'))
        ->andReturn($tablesStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine LIKE/'))
        ->andReturn($viewsStmt);
    // No SHOW CREATE for the .inner tables — those mocks would fail if asked.
    $client->shouldReceive('select')->once()
        ->with('SHOW CREATE TABLE `default`.`events`')
        ->andReturn($eventsDdl);

    $this->app->instance(ClickhouseClient::class, $client);

    $path = $this->dumpDir.'/schema.sql';
    $this->artisan('clickhouse:schema-dump', ['--path' => $path])->assertSuccessful();

    expect(file_get_contents($path))
        ->toContain('CREATE TABLE events')
        ->not->toContain('.inner.events_mv')
        ->not->toContain('.inner_id');
});

it('topologically sorts views so an MV that reads from another MV comes after it', function () {
    // Dependency chain: aaa_top reads from zzz_mid, zzz_mid reads from a real
    // table. Alphabetical order would emit aaa_top first and fail on replay.
    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'source_table'],
    ]);

    $viewsStmt = Mockery::mock(Statement::class);
    $viewsStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'aaa_top'],
        ['name' => 'zzz_mid'],
    ]);

    $sourceDdl = Mockery::mock(Statement::class);
    $sourceDdl->shouldReceive('rows')->once()->andReturn([
        ['statement' => 'CREATE TABLE default.source_table (id UInt64) ENGINE = MergeTree ORDER BY id'],
    ]);

    $aaaDdl = Mockery::mock(Statement::class);
    $aaaDdl->shouldReceive('rows')->once()->andReturn([
        ['statement' => 'CREATE MATERIALIZED VIEW default.aaa_top ENGINE = MergeTree ORDER BY id AS SELECT * FROM default.zzz_mid'],
    ]);

    $zzzDdl = Mockery::mock(Statement::class);
    $zzzDdl->shouldReceive('rows')->once()->andReturn([
        ['statement' => 'CREATE MATERIALIZED VIEW default.zzz_mid ENGINE = MergeTree ORDER BY id AS SELECT * FROM default.source_table'],
    ]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine NOT LIKE/'))
        ->andReturn($tablesStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/engine LIKE/'))
        ->andReturn($viewsStmt);
    $client->shouldReceive('select')->once()
        ->with('SHOW CREATE TABLE `default`.`source_table`')
        ->andReturn($sourceDdl);
    $client->shouldReceive('select')->once()
        ->with('SHOW CREATE TABLE `default`.`aaa_top`')
        ->andReturn($aaaDdl);
    $client->shouldReceive('select')->once()
        ->with('SHOW CREATE TABLE `default`.`zzz_mid`')
        ->andReturn($zzzDdl);

    $this->app->instance(ClickhouseClient::class, $client);

    $path = $this->dumpDir.'/schema.sql';
    $this->artisan('clickhouse:schema-dump', ['--path' => $path])->assertSuccessful();

    $contents = (string) file_get_contents($path);
    $sourcePos = strpos($contents, 'CREATE TABLE source_table');
    $zzzPos = strpos($contents, 'CREATE MATERIALIZED VIEW zzz_mid');
    $aaaPos = strpos($contents, 'CREATE MATERIALIZED VIEW aaa_top');

    expect($sourcePos)->toBeLessThan($zzzPos);
    expect($zzzPos)->toBeLessThan($aaaPos);
});
