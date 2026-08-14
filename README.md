# Jepsen tests for YugaByteDB

[YugaByteDB](https://github.com/YugaByte/yugabyte-db) is a transactional, high-performance database for building distributed cloud services developed by [YugaByte](http://www.yugabyte.com).

[Jepsen](https://github.com/aphyr/jepsen) is a testing framework for networked
databases, developed by Kyle 'Aphyr' Kingsbury to exercise and
[validate](https://jepsen.io) the claims to consistency made by database
developers or their documentation.

The tests run concurrent operations on different nodes in a YugaByteDB cluster
and checks that the operations preserve the consistency properties defined in
each test. During the tests, various combinations of nemeses can be added to
interfere with the database operations and exercise the database's consistency
protocols.

## Running

You'll need a [Jepsen
cluster](https://github.com/jepsen-io/jepsen#setting-up-a-jepsen-environment)
with at least three Debian 13 nodes. Then try:

```
lein run test -w jsql/append --isolation repeatable-read
```

This command runs a list-append test against the default version. Many
workloads and nemeses are available; see `lein run test --help` for details.

To run a full suite of tests, with various workloads and nemeses, use `lein run
test-all`:

```
lein run test-all --concurrency 4n --time-limit 300 --only-workloads-expected-to-pass
```

This spawns 4 clients per node, runs each test for 300 seconds, and chooses
only workloads and options we think should pass.

#### Workloads

The following workloads are available with `--workload` (or `-w`). Workloads
come in three groups:

- `ysql` - Workloads written specifically for the YugaByte SQL API
- `ycql` - Workloads written specifically for the YugaByte Cassandra API
- `jsql` - Workloads from jepsen.sql, which also use the YSQL API.

Some of the workloads come in different variants for different isolation
levels, like `si-` or `sz-`. See `jepsen.yugabyte.core` for the full list.

In the YSQL and YCQL groups, the following workloads are available:

- `counter` - concurrent counter increments.
- `set` - inserts single records and concurrently reads all of them back.
- `bank` - concurrent transfers between rows of a shared table.
- `long-fork` - looks for a snapshot isolation violation due to incompatible read orders.
- `single-key-acid` - each workers group is doing concurrent read, write, update-if operations on on their designated row.
- `multi-key-acid` - concurrent reads and write batches to a table with two-column composite key.

YCQL-specific tests:

- `set-index` - like set, but reads from a small pool of indices

YSQL-specific tests:

- `bank-multitable` - like bank, but across different tables.

For the `jsql` workloads, see [jepsen.sql's docs](https://github.com/jepsen-io/sql).

#### Nemeses

The following nemeses are available with `--nemesis`. Nemeses can be combined
with commas, like `--nemesis partition,clock-skew`:

- `none` - no failures
- `clock-skew` - jumps and strobes in clocks, up to hundreds of seconds
- `partition`  - all kinds of network partitions
- `partition-half` - cuts the network into two halves, one with a majority
- `partition-one` - isolate a single node
- `partition-ring` - every node sees a majority, but no node sees the same set
- `kill` - kills and restarts tservers and masters
- `kill-tserver` - kill and restart tservers
- `kill-master` - kill and restart masters
- `stop` - stops and restarts tservers and masters
- `stop-tserver` - stops and restarts tservers
- `stop-master` - stops and restarts masters
- `pause` - pauses (with SIGSTOP) and resumes (with SIGCONT) tservers and masters
- `pause-tserver` - pauses tservers
- `pause-master` - pauses masters

## License

Eclipse Public License, v1.0

Copyright YugabyteDB and Jepsen, LLC
