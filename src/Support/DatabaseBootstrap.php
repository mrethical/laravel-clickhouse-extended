<?php

declare(strict_types=1);

namespace Mrethical\LaravelClickhouseExtended\Support;

use ClickHouseDB\Client as ClickhouseClient;

/**
 * Issues `CREATE DATABASE IF NOT EXISTS` against a fresh client targeting the
 * server-default database, so the bootstrap survives the configured database
 * not yet existing.
 */
final class DatabaseBootstrap
{
    /**
     * @param  array<string, mixed>  $connectionConfig  the `clickhouse.connection` array (host/port/username/password + nested options)
     */
    public static function ensureDatabase(string $database, array $connectionConfig): void
    {
        $options = $connectionConfig['options'] ?? [];
        unset($connectionConfig['options']);

        $client = new ClickhouseClient($connectionConfig);
        $client->database('default');

        if (isset($options['timeout'])) {
            $client->setTimeout($options['timeout']);
        }

        if (isset($options['connectTimeOut'])) {
            $client->setConnectTimeOut($options['connectTimeOut']);
        }

        $quoted = '`'.str_replace('`', '``', $database).'`';
        $client->write(sprintf('CREATE DATABASE IF NOT EXISTS %s', $quoted));
    }
}
