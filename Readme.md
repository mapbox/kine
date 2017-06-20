# Kine

Kine makes reading from an aws kinesis stream easy.

Features:

 - [Kinesis Client Library]() like functionality:
  - coordination between multiple instances
  - reads from all available shards
  - start by passing in `init` and `processRecords` callbacks
  - checkpointing in dynamo


#### How to use

See [API.md](API.md) for complete reference.

```js
var Kine = require('kine');

var kcl = Kine({
  region: 'us-east-1',
  streamName: 'teststream',
  shardIteratorType: 'TRIM_HORIZON',
  table: 'teststream-kine',
  init: function(done) {
    // do initial setup, context `this` will also be available in processRecords
    console.log(this.id) // `this.id` is the shardId
    done();
  },
  processRecords: function(records, done) {
    // records is an array of records from kinesis.
    console.log(records.length);
    console.log(this.id);  // `this.id` is the shardId.

    // done(err) will throw. Restart with a process manager like upstart
    // done(null, false) with fetch more of the Kinesis stream and not checkpoint
    // done(null, true) will checkpoint, then fetch more off the Kinesis stream.
    done(null, true);
  }
});


```

##### kcl.stop

A kine instance can be halted using `stop`. This will cause any future events to bail out and
remove internal timers

```js
var Kine = require('kine');

var kcl = Kine(/* config */);

kcl.stop();
```

##### kcl.instanceInfo

An instance can be queried by record Partition Key. This allows applications to locate which
shard and instance are responsible for particular records in the stream.

```js
var Kine = require('kine');

var kcl = Kine(/* config */);

kcl.instanceInfo('0230102', function (err, info) {
  // info contains shardId, instance, hashKeyStart and hashKeyEnd
  // for the shard that contains records with partition key '0230102'
});
```
