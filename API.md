# kcl

Creates a kine client. You must provide a certain number of mandatory configuuration options, a dyno client instance
and a kinesis client instance.

**Parameters**

-   `config` **object** configuration parameters
    -   `config.shardIteratorType` **string** where to start in the stream. `LATEST`, `TRIM_HORIZON` or `AT_TIMESTAMP`
    -   `config.init` **function** function that is called when a new lease of a shard is started
    -   `config.processRecords` **function** function is that called when new records are fetches from the kinesis shard.
    -   `config.onShardClosed` **[function]** function that is called when a shard is closed
    -   `config.leaseTimeout` **[number]** time for a lease to be considered expired (default 2 minutes)
    -   `config.leaseShardDelay` **[number]** if a shard cannot be leased, try again using this delay (default 5 seconds)
    -   `config.abortKinesisRequestTimeout` **[number]** abort getRecords kinesis requests after this delay (default 5 seconds)
    -   `config.streamName` **string** the name of the kinesis stream to consume
    -   `config.maxProcessTime` **[string]** max number of milliseconds between getting records before considering a process a zombie . defaults to 300000 (5mins)
    -   `config.minProcessTime` **[string]** min number of milliseconds between getting records. defaults to 0
    -   `config.cloudwatchNamespace` **[string]** namespace to use for custom cloudwatch reporting of shard ages. required if `cloudwatchStackname` is set
    -   `config.cloudwatch` **[string]** cloudwatch instance. required if `cloudwatchNamespace` is set
    -   `config.cloudwatchStackname` **[string]** stack name to use as a dimension on custom cloudwatch reporting of shard ages. required if `cloudwatchNamespace` is set
    -   `config.verbose` **[boolean]** verbose output
    -   `config.limit` **[string]** limit used for requests to kinesis for the number of records.  This is a max, you might get few records on process records
-   `dynoClient` **object** a pre-configured dyno client to communicate with our dynamo table
-   `kinesisClient` **object** a pre-configured kinesis client to communicate with our kinesis stream

**Examples**

```javascript
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

// see /examples folder for more
```

Returns **client** a kine client
