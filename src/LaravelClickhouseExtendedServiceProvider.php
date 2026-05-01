<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended;

use ClickHouseDB\Client as ClickhouseClient;
use Illuminate\Console\Application as ConsoleApplication;
use Illuminate\Contracts\Container\Container;
use Mrethical\LaravelClickhouseExtended\ConsoleCommand\MigrateCommand;
use Mrethical\LaravelClickhouseExtended\ConsoleCommand\MigrateFreshCommand;
use Mrethical\LaravelClickhouseExtended\ConsoleCommand\SchemaDumpCommand;
use Mrethical\LaravelClickhouseExtended\ConsoleCommand\WipeCommand;
use Mrethical\LaravelClickhouseExtended\Support\SchemaInspector;
use Spatie\LaravelPackageTools\Package;
use Spatie\LaravelPackageTools\PackageServiceProvider;

class LaravelClickhouseExtendedServiceProvider extends PackageServiceProvider
{
    public function configurePackage(Package $package): void
    {
        $package
            ->name('laravel-clickhouse-extended')
            ->hasCommands([
                MigrateFreshCommand::class,
                WipeCommand::class,
                SchemaDumpCommand::class,
            ]);
    }

    public function packageRegistered(): void
    {
        // Bind SchemaInspector through the container so tests can swap it via
        // `app()->bind(SchemaInspector::class, ...)`. Accepts a `database`
        // parameter via `$container->make(SchemaInspector::class, ['database' => 'foo'])`.
        $this->app->bind(SchemaInspector::class, function (Container $container, array $params = []) {
            $client = $container->make(ClickhouseClient::class);
            $database = $params['database']
                ?? (string) $container->make('config')->get('clickhouse.connection.options.database', 'default');

            return new SchemaInspector($client, (string) $database);
        });
    }

    public function packageBooted(): void
    {
        // The upstream cybercog/laravel-clickhouse package registers its own
        // `clickhouse:migrate` command. We override it via Artisan's
        // `starting()` hook so our registration deterministically runs after
        // every provider's boot — independent of provider ordering.
        ConsoleApplication::starting(function (ConsoleApplication $artisan): void {
            $artisan->resolve(MigrateCommand::class);
        });
    }
}
