<?php

declare(strict_types=1);

use ClickHouseDB\Client as ClickhouseClient;
use ClickHouseDB\Statement;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Event;
use Mrethical\LaravelClickhouseExtended\Events\ClickhouseDatabaseRefreshed;

it('skips wipe when migrations log table does not exist', function () {
    $existsStmt = Mockery::mock(Statement::class);
    $existsStmt->shouldReceive('rows')->once()->andReturn([['result' => 0]]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/EXISTS TABLE `default`\\.`migrations`/'))
        ->andReturn($existsStmt);
    $this->app->instance(ClickhouseClient::class, $client);

    $migrateRan = false;
    Artisan::command('clickhouse:migrate {--database=} {--force} {--path=*} {--realpath} {--schema-path=} {--step=}', function () use (&$migrateRan) {
        $migrateRan = true;

        return 0;
    });

    Event::fake([ClickhouseDatabaseRefreshed::class]);

    $this->artisan('clickhouse:migrate-fresh', ['--force' => true])->assertSuccessful();

    expect($migrateRan)->toBeTrue();
    Event::assertDispatched(ClickhouseDatabaseRefreshed::class);
});

it('delegates to clickhouse:wipe when migrations log table exists', function () {
    $existsStmt = Mockery::mock(Statement::class);
    $existsStmt->shouldReceive('rows')->once()->andReturn([['result' => 1]]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/EXISTS TABLE `default`\\.`migrations`/'))
        ->andReturn($existsStmt);
    $this->app->instance(ClickhouseClient::class, $client);

    $wipeArgs = null;
    Artisan::command('clickhouse:wipe {--database=} {--force}', function () use (&$wipeArgs) {
        $wipeArgs = [
            'database' => $this->option('database'),
            'force' => $this->option('force'),
        ];

        return 0;
    });

    Artisan::command('clickhouse:migrate {--database=} {--force} {--path=*} {--realpath} {--schema-path=} {--step=}', fn () => 0);

    Event::fake([ClickhouseDatabaseRefreshed::class]);

    $this->artisan('clickhouse:migrate-fresh', ['--force' => true])->assertSuccessful();

    expect($wipeArgs)->not->toBeNull();
    expect($wipeArgs['force'])->toBeTrue();

    Event::assertDispatched(ClickhouseDatabaseRefreshed::class);
});

it('forwards path/realpath/schema-path/step to clickhouse:migrate', function () {
    $existsStmt = Mockery::mock(Statement::class);
    $existsStmt->shouldReceive('rows')->once()->andReturn([['result' => 0]]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/EXISTS TABLE `default`\\.`migrations`/'))
        ->andReturn($existsStmt);
    $this->app->instance(ClickhouseClient::class, $client);

    $migrateArgs = null;
    Artisan::command('clickhouse:migrate {--database=} {--force} {--path=*} {--realpath} {--schema-path=} {--step=}', function () use (&$migrateArgs) {
        $migrateArgs = [
            'path' => $this->option('path'),
            'realpath' => $this->option('realpath'),
            'schema-path' => $this->option('schema-path'),
            'step' => $this->option('step'),
        ];

        return 0;
    });

    $this->artisan('clickhouse:migrate-fresh', [
        '--force' => true,
        '--path' => ['/tmp/migrations-a', '/tmp/migrations-b'],
        '--realpath' => true,
        '--schema-path' => '/tmp/schema.sql',
        '--step' => 2,
    ])->assertSuccessful();

    expect($migrateArgs['path'])->toBe(['/tmp/migrations-a', '/tmp/migrations-b']);
    expect($migrateArgs['realpath'])->toBeTrue();
    expect($migrateArgs['schema-path'])->toBe('/tmp/schema.sql');
    expect((int) $migrateArgs['step'])->toBe(2);
});
