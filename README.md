# Flink State Management Playground
This project contains a very simple pipeline to allow for experimenting with the Flink state management API. There are 
two entrypoints within this project: `Job` and `StateGC`. `Job` is responsible for running the pipeline and creating savepoints
while `StateGC` can be used to filter out state that is no longer needed.

## Operators

### SensorManager
Responsible for receiving configuration information about a sensor and propagating that information via broadcast
to and operators that require it. 

#### State
Has no state.

### ReadingFilter
Filters out readings from sensors that have been disabled.

#### State
Stores broadcast state to know which sensors have been enabled or disabled and uses it as a whitelist
to pass messages down the pipeline.

### ReadingAverager
Receives messages from ReadingFilter and averages the readings keyed/grouped by sensor ID. 

#### State
Stores broadcast state and keyed state. Keeps a running average of sensor readings and then enriches
they raw output with more data from broadcast state.

## Issues

### Lack of KeyedBroadcastStateBootstrapFunction
The two options currently available to bootstrap a savepoint with state are [KeyedStateBootstrapFunction](https://ci.apache.org/projects/flink/flink-docs-release-1.9/api/java/org/apache/flink/state/api/functions/KeyedStateBootstrapFunction.html) and 
[BroadcastStateBootstrapFunction](https://ci.apache.org/projects/flink/flink-docs-release-1.9/api/java/org/apache/flink/state/api/functions/BroadcastStateBootstrapFunction.html).
Because these are the only two options, it's not possible to bootstrap both keyed and broadcast state for one operator.

### Complex Key Causes Serialization Error _(see `key-by-case-class` branch)_
Having operators keyed by a case class causes the below exception:
```shell script
Caused by: org.apache.flink.util.StateMigrationException: The new key serializer must be compatible.
	at org.apache.flink.contrib.streaming.state.restore.AbstractRocksDBRestoreOperation.readMetaData(AbstractRocksDBRestoreOperation.java:194)
	at org.apache.flink.contrib.streaming.state.restore.RocksDBFullRestoreOperation.restoreKVStateMetaData(RocksDBFullRestoreOperation.java:170)
	at org.apache.flink.contrib.streaming.state.restore.RocksDBFullRestoreOperation.restoreKeyGroupsInStateHandle(RocksDBFullRestoreOperation.java:157)
	at org.apache.flink.contrib.streaming.state.restore.RocksDBFullRestoreOperation.restore(RocksDBFullRestoreOperation.java:141)
	at org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackendBuilder.build(RocksDBKeyedStateBackendBuilder.java:270)
	... 13 more
```

This error is seen after trying to read from a savepoint that was created using the same case class as a key.

## Useful links
- Flink's summary for the State Processor API [here](https://flink.apache.org/feature/2019/09/13/state-processor-api.html)
- Flink's writeup of the State Processor API [here](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/libs/state_processor_api.html#writing-new-savepoints)
- Flink API [here](https://ci.apache.org/projects/flink/flink-docs-release-1.9/api/java/)

## Useful commands
### Startup Flink and RabbitMQ, and then make the state folder writeable
```shell script
dc up -d
dc exec jobmanager bash
chown flink:flink /state
``` 

### Create a savepoint
```shell script
flink savepoint -m localhost:8081 :jobId /state
```

### Locally:
- `PORT`: is randomly generated when the pipeline starts up in IntelliJ
- `JOB_ID`: is returned from the `list` command

```shell script
flink list -m localhost:PORT
flink savepoint -m localhost:PORT JOB_ID $(pwd)/state
```
