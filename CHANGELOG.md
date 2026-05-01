# Changelog

All notable changes to `laravel-clickhouse-extended` will be documented in this file.

## v0.1.0 — 2026-05-01

Initial release.

- `clickhouse:migrate` — overrides upstream, supports `--schema-path` for replaying a schema dump on first migrate.
- `clickhouse:migrate-fresh` — drops everything, then runs migrations.
- `clickhouse:wipe` — drops every view, table, and dictionary.
- `clickhouse:schema-dump` — emits a portable, DB-agnostic dump (strips source-DB qualifiers, topo-sorts views by dependency, skips `.inner.*` storage tables).
- `Mrethical\LaravelClickhouseExtended\Testing\ClickhouseTruncation` trait — Clickhouse equivalent of Laravel's `DatabaseTruncation`, with parallel-testing support.
