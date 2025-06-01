# Litebeam

Divide your SQLite files like a beam of light!

## What is Litebeam

A lightweight Go module for managing SQLite database sharding with minimal setup and maximum control.

## What is it used for

Litebeam helps you split your SQLite data across multiple physical database files (shards). This is useful when you have logically separated data (e.g., per-user, per-tenant) that does not need to be queried across entities.

> **Note**: This is a niche use case. Most systems are better served by a single client/server database. However, for cost-sensitive or low-complexity applications, sharded SQLite can be a pragmatic option.

## Why shard?

SQLite’s biggest limitation is its single-writer constraint. While Write-Ahead Logging (WAL) and other optimizations help, heavy concurrent writes eventually cause contention.

Litebeam’s approach is to split your data logically into shards — separate SQLite database files — distributing write load and reducing contention.

It is not a replacement for Postgres or other client/server databases but increases the capacity and concurrency limits of SQLite in many real-world scenarios.

> I would argue 90% of apps never need a full client/server database. Simplicity and "good enough" often win.

## What Litebeam does NOT do

- Fix bad schema designs.
- Manage database transactions or connection lifecycles.
- Provide backup or replication strategies.

These responsibilities are left to the user or external tooling.

## Backup guide

Backup and replication strategies vary widely. Litebeam focuses on sharding and lets you choose how to handle backups per shard.

### Litespeed + on-startup Litebeam

> TODO: guide coming soon.

## Contribution Policy

Litebeam is primarily a personal project. Contributions for bug fixes and tests are welcome. Feature requests can be opened as issues, but major feature development is unlikely.

Feel free to fork and extend as needed. The codebase is small (~500 lines) and straightforward.

## State of tests

Current tests cover essential functionality for my use cases and will expand over time. They are adequate but minimal.