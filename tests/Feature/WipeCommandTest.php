<?php

declare(strict_types=1);

use ClickHouseDB\Client as ClickhouseClient;
use ClickHouseDB\Statement;

it('drops every view, table, and dictionary in the configured database', function () {
    $viewsStmt = Mockery::mock(Statement::class);
    $viewsStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'user_summary_view'],
    ]);

    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'users'],
        ['name' => 'events'],
    ]);

    $dictStmt = Mockery::mock(Statement::class);
    $dictStmt->shouldReceive('rows')->once()->andReturn([
        ['name' => 'country_codes'],
    ]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern("/engine LIKE '%View'/"))
        ->andReturn($viewsStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern("/engine NOT LIKE '%View'/"))
        ->andReturn($tablesStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern('/system\\.dictionaries/'))
        ->andReturn($dictStmt);
    $client->shouldReceive('write')->once()->with('DROP TABLE IF EXISTS `default`.`user_summary_view`');
    $client->shouldReceive('write')->once()->with('DROP TABLE IF EXISTS `default`.`users`');
    $client->shouldReceive('write')->once()->with('DROP TABLE IF EXISTS `default`.`events`');
    $client->shouldReceive('write')->once()->with('DROP DICTIONARY IF EXISTS `default`.`country_codes`');

    $this->app->instance(ClickhouseClient::class, $client);

    $this->artisan('clickhouse:wipe', ['--force' => true])->assertSuccessful();
});

it('targets a custom database when --database is given', function () {
    $viewsStmt = Mockery::mock(Statement::class);
    $viewsStmt->shouldReceive('rows')->once()->andReturn([]);

    $tablesStmt = Mockery::mock(Statement::class);
    $tablesStmt->shouldReceive('rows')->once()->andReturn([['name' => 'logs']]);

    $dictStmt = Mockery::mock(Statement::class);
    $dictStmt->shouldReceive('rows')->once()->andReturn([]);

    $client = Mockery::mock(ClickhouseClient::class);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern("/database = 'analytics'.+engine LIKE/"))
        ->andReturn($viewsStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern("/database = 'analytics'.+engine NOT LIKE/"))
        ->andReturn($tablesStmt);
    $client->shouldReceive('select')->once()
        ->with(Mockery::pattern("/system\\.dictionaries.+database = 'analytics'/"))
        ->andReturn($dictStmt);
    $client->shouldReceive('write')->once()->with('DROP TABLE IF EXISTS `analytics`.`logs`');

    $this->app->instance(ClickhouseClient::class, $client);

    $this->artisan('clickhouse:wipe', ['--force' => true, '--database' => 'analytics'])
        ->assertSuccessful();
});
