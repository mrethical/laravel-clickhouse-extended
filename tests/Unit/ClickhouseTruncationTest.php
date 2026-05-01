<?php

declare(strict_types=1);

use ClickHouseDB\Client as ClickhouseClient;
use ClickHouseDB\Statement;
use Illuminate\Support\Facades\Artisan;
use Mrethical\LaravelClickhouseExtended\Testing\ClickhouseTruncation;
use Mrethical\LaravelClickhouseExtended\Testing\ClickhouseTruncationState;

beforeEach(function () {
    ClickhouseTruncationState::$migrated = false;
});

afterEach(function () {
    ClickhouseTruncationState::$migrated = false;
});

function makeTruncationConsumer($app, array $extras = []): object
{
    $consumer = new class
    {
        use ClickhouseTruncation;

        public $app;

        public array $artisanCalls = [];

        public $tokenOverride = false;

        public function artisan(string $command, array $params = []): int
        {
            $this->artisanCalls[] = [$command, $params];

            return Artisan::call($command, $params);
        }

        protected function clickhouseTestToken()
        {
            return $this->tokenOverride;
        }
    };
    $consumer->app = $app;

    foreach ($extras as $key => $value) {
        $consumer->{$key} = $value;
    }

    return $consumer;
}

it('runs clickhouse:migrate-fresh on first invocation and skips truncation', function () {
    $migrateFreshRan = false;
    Artisan::command('clickhouse:migrate-fresh {--force} {--database=} {--drop-dictionaries} {--step=}', function () use (&$migrateFreshRan) {
        $migrateFreshRan = true;

        return 0;
    });

    $emptyMvs = Mockery::mock(Statement::class);
    $emptyMvs->shouldReceive('rows')->once()->andReturn([]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern("/engine = 'MaterializedView'/"))
        ->andReturn($emptyMvs);
    $client->shouldNotReceive('write');
    $this->app->instance(ClickhouseClient::class, $client);

    $consumer = makeTruncationConsumer($this->app);

    $consumer->setUpClickhouseTruncation();

    expect($migrateFreshRan)->toBeTrue();
    expect(ClickhouseTruncationState::$migrated)->toBeTrue();
    expect($consumer->artisanCalls[0][0])->toBe('clickhouse:migrate-fresh');
    expect($consumer->artisanCalls[0][1])->toBe(['--force' => true]);
});

it('stops every materialized view after first migrate', function () {
    Artisan::command('clickhouse:migrate-fresh {--force} {--database=} {--drop-dictionaries} {--step=}', fn () => 0);

    $mvs = Mockery::mock(Statement::class);
    $mvs->shouldReceive('rows')->once()->andReturn([
        ['name' => 'user_signups_daily'],
        ['name' => 'event_counts_hourly'],
    ]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()->andReturn($mvs);
    $client->shouldReceive('write')->once()->with('SYSTEM STOP VIEW `default`.`user_signups_daily`');
    $client->shouldReceive('write')->once()->with('SYSTEM STOP VIEW `default`.`event_counts_hourly`');

    $this->app->instance(ClickhouseClient::class, $client);

    $consumer = makeTruncationConsumer($this->app);
    $consumer->setUpClickhouseTruncation();

    expect(true)->toBeTrue();
});

it('skips the stop-view step when stopRefreshableViews is false', function () {
    Artisan::command('clickhouse:migrate-fresh {--force} {--database=} {--drop-dictionaries} {--step=}', fn () => 0);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldNotReceive('select');
    $client->shouldNotReceive('write');
    $this->app->instance(ClickhouseClient::class, $client);

    $consumer = makeTruncationConsumer($this->app, ['stopRefreshableViews' => false]);
    $consumer->setUpClickhouseTruncation();

    expect(ClickhouseTruncationState::$migrated)->toBeTrue();
});

it('switches to a per-token database when running under parallel testing', function () {
    config()->set('clickhouse.connection', [
        'host' => '127.0.0.1',
        'port' => 8123,
        'username' => 'default',
        'password' => '',
        'options' => ['database' => 'default', 'timeout' => 1, 'connectTimeOut' => 2],
    ]);

    $bootstrapCalled = false;
    Artisan::command('clickhouse:migrate-fresh {--force} {--database=} {--drop-dictionaries} {--step=}', function () use (&$bootstrapCalled) {
        $bootstrapCalled = true;

        expect(config('clickhouse.connection.options.database'))->toBe('default_3');

        return 0;
    });

    $emptyMvs = Mockery::mock(Statement::class);
    $emptyMvs->shouldReceive('rows')->andReturn([]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->andReturn($emptyMvs);
    $client->shouldReceive('write')->andReturnNull();
    $this->app->instance(ClickhouseClient::class, $client);
    $this->app->bind(ClickhouseClient::class, fn () => $client);

    $consumer = makeTruncationConsumer($this->app, ['tokenOverride' => '3']);

    // The DatabaseBootstrap helper uses a fresh ClickhouseClient targeting the
    // server-default DB; we can't intercept that without a real server, so swap
    // the trait method via a lightweight subclass.
    $consumer = new class extends stdClass
    {
        use ClickhouseTruncation;

        public $app;

        public $artisanCalls = [];

        public $tokenOverride = '3';

        public bool $bootstrapped = false;

        public function artisan(string $command, array $params = []): int
        {
            $this->artisanCalls[] = [$command, $params];

            return Artisan::call($command, $params);
        }

        protected function clickhouseTestToken()
        {
            return $this->tokenOverride;
        }

        protected function ensureClickhouseDatabaseForCurrentTestToken(): void
        {
            $base = (string) $this->app['config']->get('clickhouse.connection.options.database', 'default');
            $database = $base.'_'.$this->clickhouseTestToken();
            $this->app['config']->set('clickhouse.connection.options.database', $database);
            $this->bootstrapped = true;
        }
    };
    $consumer->app = $this->app;

    $consumer->setUpClickhouseTruncation();

    expect($bootstrapCalled)->toBeTrue();
    expect($consumer->bootstrapped)->toBeTrue();
});

it('truncates every truncatable table except the migrations log on subsequent runs', function () {
    ClickhouseTruncationState::$migrated = true;

    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'users'],
        ['name' => 'events'],
        ['name' => 'migrations'],
    ]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/system\\.tables.+engine NOT LIKE/'))
        ->andReturn($tablesStmt);
    $client->shouldReceive('write')->once()->with('TRUNCATE TABLE IF EXISTS `default`.`users`');
    $client->shouldReceive('write')->once()->with('TRUNCATE TABLE IF EXISTS `default`.`events`');
    $client->shouldNotReceive('write')->with(Mockery::pattern('/`migrations`/'));

    $this->app->instance(ClickhouseClient::class, $client);

    $consumer = makeTruncationConsumer($this->app);
    $consumer->setUpClickhouseTruncation();

    expect(true)->toBeTrue();
});

it('respects an explicit tablesToTruncate whitelist', function () {
    ClickhouseTruncationState::$migrated = true;

    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'users'],
        ['name' => 'events'],
        ['name' => 'only_me'],
    ]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()->andReturn($tablesStmt);
    $client->shouldReceive('write')->once()->with('TRUNCATE TABLE IF EXISTS `default`.`only_me`');
    $client->shouldNotReceive('write')->with(Mockery::pattern('/`users`|`events`/'));

    $this->app->instance(ClickhouseClient::class, $client);

    $consumer = makeTruncationConsumer($this->app, ['tablesToTruncate' => ['only_me']]);
    $consumer->setUpClickhouseTruncation();

    expect(true)->toBeTrue();
});

it('re-applies the per-token database swap on every test, including subsequent runs', function () {
    // Regression: the container is rebuilt between tests, which re-reads
    // clickhouse.connection.options.database from env and undoes the
    // previous worker swap. ensureClickhouseDatabaseForCurrentTestToken()
    // must therefore run per test, not only on the first invocation.
    ClickhouseTruncationState::$migrated = true;

    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()->andReturn($tablesStmt);
    $this->app->instance(ClickhouseClient::class, $client);

    $consumer = new class
    {
        use ClickhouseTruncation;

        public $app;

        public array $artisanCalls = [];

        public $tokenOverride = '7';

        public int $bootstrapCount = 0;

        public function artisan(string $command, array $params = []): int
        {
            $this->artisanCalls[] = [$command, $params];

            return Artisan::call($command, $params);
        }

        protected function clickhouseTestToken()
        {
            return $this->tokenOverride;
        }

        protected function ensureClickhouseDatabaseForCurrentTestToken(): void
        {
            $this->bootstrapCount++;
        }
    };
    $consumer->app = $this->app;

    $consumer->setUpClickhouseTruncation();

    expect($consumer->bootstrapCount)->toBe(1);
});

it('respects an exceptTables blacklist alongside the migrations log', function () {
    ClickhouseTruncationState::$migrated = true;

    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'users'],
        ['name' => 'audit_log'],
        ['name' => 'migrations'],
    ]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()->andReturn($tablesStmt);
    $client->shouldReceive('write')->once()->with('TRUNCATE TABLE IF EXISTS `default`.`users`');
    $client->shouldNotReceive('write')->with(Mockery::pattern('/`audit_log`|`migrations`/'));

    $this->app->instance(ClickhouseClient::class, $client);

    $consumer = makeTruncationConsumer($this->app, ['exceptTables' => ['audit_log']]);
    $consumer->setUpClickhouseTruncation();

    expect(true)->toBeTrue();
});
