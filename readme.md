# Litebeam

Divide your SQLite files like a beam of light!

## What is Litebeam
A light weight golang module to help with the management of sqlite sharding

## What is it used for

To create shards of SQLite databases for non-related or loosely related items -> For example: user data that doesn't need to be queried against other users. Litebeam helps manage the creation of these files.

> Note: This is a very niche use case and is not recommended for most systems. Consider carefully if the complexity of sharding outweighs the benefits for your specific workload. Standard client/server databases are often a better fit for complex relational data or high levels of concurrent write access across different data domains. SQLITE is a great cost saver in the short - midterm.

## Why shard?

The biggest limitation for SQLite is the single-writer limitation. While you can get away with one file for a long time using WAL mode and other optimizations, sooner or later you may run into write contention or throughput limitations (if your app gets big enough and performs many concurrent writes).

That is the purpose of Litebeam's targeted use case: by splitting data logically into separate database files (shards), you distribute the write load, allowing multiple writers to operate concurrently, each on a different shard. It isn't a replacement for Postgres or other client/server databases; however, it increases the length of the runway until those solutions become a necessity for certain types of applications. Litebeam streamlines the setup for this sharded approach.

> I would argue 90% of all apps do not require a postgres server and never reach a level where they require a client/server database (I am pulling that 90% number out of thin air, but I would wadger a pretty penny it is around here, we are all just so used to comparing our apps to giants, so we reach for what they use) instead I believe in simplicty and "good enough". I feel that we should stop and think about if we require these complicated solutions to what should be a simple task.

Sqlite isn't perfect. It has some serious downsides. That all said, it is an amazing database and can help reduce costs during an initial startup phase.

And because it is "sql", transfering the data across into postgres or other client/server databases is not too complex, making migration a more manageable task

## What doesn't this do?
- It cannot help you with bad schema -> that is for you to work out.
- Nor will it make sure your database tranactions close and open properly. Again that is on you.
- It won't back up your shards -> that is for you to work out too. But I do have a guide (TODO)

## Backup guide

Backup and replication are important factors for production ready services. Litebeam has been thought out to help make this as simple as possible by only focusing on the sharding. This gives you the flexibility to use a backup/replication strategy that fits your needs

### Litespeed + on-startup Litebeam
>TODO

## Contribution Policy
Litebeam is open to code contributions for bug fixes and test files only. This is more a personal library that I thought might help some people working on similar issues regarding sqlite sharding so I do not want to get bogged down in maintaining features. Please submit an issue if you have a feature you'd like to request. 

Feel free to fork and make it more robust for your use cases! It is only around 500 lines or so.

### State of tests
Right now the tests are pretty barebones -> I am only testing for my usecases at this time, but will expand them over time. They are "good enough" for my needs at this time.