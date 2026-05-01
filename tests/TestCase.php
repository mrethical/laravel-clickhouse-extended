<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\Tests;

use Cog\Laravel\Clickhouse\ClickhouseServiceProvider;
use Mrethical\LaravelClickhouseExtended\LaravelClickhouseExtendedServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

class TestCase extends Orchestra
{
    protected function getPackageProviders($app): array
    {
        return [
            ClickhouseServiceProvider::class,
            LaravelClickhouseExtendedServiceProvider::class,
        ];
    }
}
