# Kine [![Build Status](https://travis-ci.org/mapbox/kine.svg?branch=master)](https://travis-ci.org/mapbox/kine)

Kine makes reading from an AWS kinesis stream easy by providing AWS kcl-like (kinesis client library) functionality.

## Installation

To install kine for use in your application, run:

```bash
npm install @mapbox/kine
```

To use the CLI, see instructions further below.

## Quickstart

```js
const Kine = require('@mapbox/kine').kcl;
const Sync = require('@mapbox/kine').sync;
const Dyno = require('@mapbox/dyno');

const kinesisClient = new AWS.Kinesis();
const dynoClient = Dyno({ region: 'us-east-1', table: 'myTableName'});

// start a sync instance to keep your stream state in sync with dynamo
// you probably want this code in a separate process/worker to keep it isolated from worker failures
const sync = Sync(syncConfig, dynoClient, kinesisClient);
sync.init(function () { // init dynamo table
    sync.shards(); // start syncing in a loop
})

// start a consumer worker for a single shard
// kine will start looking for available shards, pick one if possible and start pulling records immediately.
Kine({
  streamName: 'my-kinesis-stream',
  shardIteratorType: 'TRIM_HORIZON',
  init: function (cb) {
      cb(null, null);
  },
  processRecords: function (records, cb) {
      // records is an array of records from kinesis.
      records.forEach(function (r) {
        console.log(r.Data.toString());
      });
   // cb(err) will throw. Restart with a process manager like upstart
   // cb(null, false) with fetch more of the Kinesis stream and not checkpoint
   // cb(null, true) will checkpoint, then fetch more off the Kinesis stream.
   cb(null, true);
  }
}, dynoClient, kinesisClient);
```

See [API.md](API.md) for complete reference and all available options.

## How kine works

Kine comes bundled with 2 parts:
- a kcl (kinesis client library) which you instantiate once for each shard to be read from
- a small sync module to keep your kinesis stream synchronized with a dynamo table. Your kcl instances will interact with this
dynamo table to track state. For example, if your stream gets resharded, the new shards will get synced to your table automatically.

Typically your application would be set up in a fashion similar to this:

![kine-macro](https://user-images.githubusercontent.com/1378612/30515194-7d0f0490-9af1-11e7-997b-a864ce3b6736.png)

Where each worker would be individually going through the following steps:

![kine-micro](https://user-images.githubusercontent.com/1378612/30515193-7d0d2e04-9af1-11e7-9831-7cab10e8b870.png)

## Kine's shared responsibility

Every kine instance reads data from a single shard. It is responsible for:
- Finding a free shard to lease, then leasing it.
- Calling your `init`, `processRecords` and `onShardClosed` functions.
- Giving your application a "heartbeat" by updating the shard lease every few seconds in dynamo. Without this, a lease
may expire and another worker/process would jump in to take over that lease.
- Checkpointing the last processed record in dynamo. If your process dies unexpectedly, the next one will resume where
 the previous one died off.
- Continuously calling getRecords for you in a loop – but not too fast (to respect AWS limits). Knowing what to do when
AWS returns a bad, incomplete or truncated response.
- Marking a shard as complete and exiting gracefully based on the NextShardIterator returned.
- Sending CloudWatch metrics back to AWS to know more about your application's performance (if configured).

You are responsible for:
- Provisioning at least as many workers running kine as your stream has shards. For example, if you have 8 shards on
a stream, you should always have a minimum of 8 workers running.
- Providing functions for different events: `init`, `processRecords` and `onShardClosed` if applicable.
- Providing configuration options to kine, including a pre-configured Dyno instance, a Kinesis client and a CloudWatch
 client if applicable.

## Using the CLI

The CLI can be useful to debug data in a stream, or pull records on your local machine for inspection.
It does not support pulling from a local kinesis stream currently.

To have access to the kine CLI, make sure you have it installed:

```bash
$ npm install -g @mapbox/kine
# or
$ git clone https://github.com/mapbox/kine && cd kine && npm link
```

**Important!** Kine assumes your shell has proper AWS credentials configured to access resources in your account.
At a minimum, you should have an access key id and secret key that have permissions to read from your stream.

```
AWS_ACCESS_KEY_ID=XXXXX
AWS_SECRET_ACCESS_KEY=YYYYYY
AWS_DEFAULT_REGION=us-east-1
```

### ls

Display information about a kinesis stream and its shards.

Parameters:

- `stream name`
  - description: the name of your kinesis stream
  - required: yes
  - default: n/a

#### Examples

```
kine ls StreamName
```

![screen shot 2017-07-08 at 10 55 41 am](https://user-images.githubusercontent.com/1378612/27987974-1334aebe-63cc-11e7-8a2a-86ec6834d3b8.png)

Note: Cloudwatch stats will only show if you have enabled [shard-level metrics](https://docs.aws.amazon.com/streams/latest/dev/monitoring-with-cloudwatch.html#kinesis-metrics-shard) on your stream.

<hr />

### pull

Get records for one or many shards in a stream

Parameters:

- `stream name`
  - description: the name of your kinesis stream
  - required: yes
  - default: n/a
- `-t`
  - description: the type of shard iterator. Can be one of `LATEST`, `TRIM_HORIZON` or `AT_TIMESTAMP`
  - required: no
  - default: `LATEST`
- `-d`
  - description: if `-t` is `AT_TIMESTAMP`, the timestamp to start reading at
  - required: yes, if `-t` is `AT_TIMESTAMP`. Otherwise, no.
  - default: n/a
- `-i`
  - description: specify a shard id to read from
  - required: no
  - default: kine will read from all shards if `-i` is not present
- `-o`
  - description: create a folder to write the result. Inside this folder, every shard will have a file named after its shard id.
  - required: no
  - default: output goes to stdout
- `-z`
  - description: how fast should be pull from shards on a stream (1, 2 or 3 second timeout). Can be one of `slow`, `normal` or `fast`
  - required: no
  - default: `normal`

#### Examples

```
kine pull StreamName
```
- `LATEST` shardIterator type
- pull from all shards at once
- reads at `normal` speed
- outputs to stdout

```
kine pull StreamName -i shardId-000000000001
```
- same as above, but pull from shard `shardId-000000000001` only

```
kine pull StreamName -t TRIM_HORIZON -z fast -i shardId-000000000001
```
- `TRIM_HORIZON` shardIterator type
- wait only 1 second between pulls on each shard
- outputs to stdout

```
kine pull StreamName -t AT_TIMESTAMP -d 1499476500000
```
- start reading at timestamp `1499476500000`

```
kine pull StreamName -t TRIM_HORIZON -i shardId-000000000001 -o output

Found 0 records in shardId-000000000001
Found 1 records in shardId-000000000001. ms behind is 86399000
Found 501 records in shardId-000000000001. ms behind is 86382000
Found 1000 records in shardId-000000000001. ms behind is 86371000
Found 1000 records in shardId-000000000001. ms behind is 86221000
Found 1000 records in shardId-000000000001. ms behind is 86217000
Found 500 records in shardId-000000000001. ms behind is 86221000
...
$ ls -lah output
$ .
$ ..
$ 27M shardId-000000000001.json
```
- `TRIM_HORIZON` shardIterator type
- pull from shard `shardId-000000000001` only
- create a folder called `output`, containing a single file called `shardId-000000000001.json`

This command will also provide stats to stdout on current progress while writing to folder.

## Running an example app locally

To run an example app locally, install dependencies, then run `examples/localExample.js`. More info and comments in
the source file. You should see output resembling this:

```
# init
Dynalite started on port 4568, Kinesalite started on port 5568, created stream teststream
[Sat, 16 Sep 2017 18:58:22 GMT] [info] [kine] [ Sync ] syncInit
[Sat, 16 Sep 2017 18:58:22 GMT] [info] [kine] [ Sync ] shardSync
shards have synced once, state is [{"ShardId":"shardId-000000000000","HashKeyRange":{"StartingHashKey":"0","EndingHashKey":"340282366920938463463374607431768211455"},"SequenceNumberRange":{"StartingSequenceNumber":"49577069164543465601764791612224223308824083975458783234"}}]
[Sat, 16 Sep 2017 18:58:22 GMT] [info] [default] [ Kcl ] init
[Sat, 16 Sep 2017 18:58:22 GMT] [info] [default] [ Kcl ] availableShard shardId-000000000000
[Sat, 16 Sep 2017 18:58:22 GMT] [info] [shardId-000000000000] [ Kcl ] noShardsAvailable
...and we have liftoff!
30 bottles of beer... on the wall
63 bottles of beer... on the wall
14 bottles of beer... on the wall
75 bottles of beer... on the wall
[Sat, 16 Sep 2017 18:58:27 GMT] [info] [shardId-000000000000] [ Kcl ] noShardsAvailable
2 bottles of beer... on the wall
```

## Running an example app on AWS

## Tests

To run the test suite, use:

```bash
npm test
```