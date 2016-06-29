#Kine

Kine makes reading from an aws kinesis stream easy.

Features:

 - [Kinesis Client Library]() like functionality:
  - coordination between multiple instances
  - reads from all available shards
  - start by passing in `init` and `processRecords` callbacks
  - checkpointing in dynamo


#### How to use


```js
var Kine = require('kine');

kine2 = Kine({
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
