These are scripts that YugaByte uses to run the test in their environment.
Since I don't have access to this environment I'm not sure how actively
maintained they are. I'm preserving this in case YugaByte wants to try and
merge their fork with this one later. The following is the README state from
2028-08-14.

#### YugaByteDB cluster setup

- Create YugaByteDB cluster with 5 nodes and replication factor of 3.
- Create text file `~/code/jepsen/nodes` and list all cluster nodes there - one per line, for example:
```bash
yb-test-jepsen-n1
yb-test-jepsen-n2
yb-test-jepsen-n3
yb-test-jepsen-n4
yb-test-jepsen-n5
```
- Setup cluster nodes for running Jepsen tests:
```bash
~/code/jepsen/yugabyte/setup-jepsen.sh
```

#### Wrapper scripts

These wrapper scripts were written for YugaByte's version of these tests, and
may no longer work correctly. They're preserved here in case anyone would like
to use them going forward. They aren't necessary to run the tests; the CLI interface for these tests can run all tests automatically.

All commands described below should be run in `~/code/jepsen/yugabyte` directory.

In order to display help and see available tests and nemeses:
```bash
lein run test --help
```

To run test with specific nemesis, for example `start-stop-master`:
```bash
lein run test --nodes-file ~/code/jepsen/nodes --nemesis start-stop-master
```

Wrapper to manage log files seperately and summarize tests, for use in automated jenkins testing:
```bash
./run-jepsen.py
```

This will also classify test results by categories and put them into `~/code/jepsen/yugabyte/results-sorted` 
sub-directories:
- *ok*
- *timed-out* - test run (including analysis phase) took more than time limit defined in `run-jepsen.py`.
- *no-history* - file with operations history is absent.
- *valid-unknown* - test results checker wasn't able to determine whether results are valid. 
- *invalid* - history of operations is inconsisent.

