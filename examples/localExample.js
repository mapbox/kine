'use strict';

/**
 * To run this script, make sure to "npm install" dependencies, and then "node localExample.js". It will set up fake
 * local resources, insert data into them, and pull them back out using a kine instance.
 */

const Kcl = require('../index.js').kcl;
const Sync = require('../index.js').sync;
const test = require('tape');
const Dyno = require('@mapbox/dyno');
const queue = require('d3-queue').queue;
const AWS = require('aws-sdk');
const util = require('../test/utils');

/***
 * start a basic KCL that pulls from our single-shard stream
 */

function startPulling() {
  const config = {
    streamName: syncConfig.streamName,
    shardIteratorType: 'TRIM_HORIZON',
    init: function (cb) {
      console.log('...and we have liftoff!');
      cb(null, null);
    },
    processRecords: function (records, cb) {
      records.forEach(function (r) {
        console.log(r.Data.toString(), 'on the wall');
      });
      cb(null, true); //true means we want to checkpoint
    },

    // everything beyond this point is optional
    verbose: false,
    leaseTimeout: 120000,
    limit: 20,
    minProcessTime: 1000,
    maxProcessTime: 300000,
    leaseShardDelay: 5000,
    abortKinesisRequestTimeout: 4000
  };
  // start the Kcl
  Kcl(config, dynoClient, kinesisClient);
}

/***
 * Given that our stream is single-shard, we can start another KCL to show how our lease mechanism will block this
 * second KCL from accessing the shard. You should see [noShardsAvailable] messages in the output. Until our first KCL
 * dies or releases the lease, this second KCL will hang around and wait, trying to lease a new shard every 5 seconds.
 */
function startPullingOther() {
  const config = {
    verbose: true,
    streamName: syncConfig.streamName,
    shardIteratorType: 'TRIM_HORIZON',
    init: function (cb) {
      cb(null, null);
    },
    processRecords: function (records, cb) {
      cb(null, true);
    }
  };
  // start another Kcl
  Kcl(config, dynoClient, kinesisClient);
}

/***
 * Setup base resources
 */

process.env.AWS_REGION = 'us-east-1';
process.env.AWS_ACCESS_KEY_ID = 'does not matter';
process.env.AWS_SECRET_ACCESS_KEY = 'does not matter';
process.env.shardCount = 1;

const kinesisOptions = {
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  endpoint: 'http://localhost:5568'
};

const kinesisClient = new AWS.Kinesis(kinesisOptions);
const syncConfig = {
  verbose: true,
  table: 'kine-kcl-test',
  streamName: 'teststream'
};
const dynoClient = Dyno({
  endpoint: 'http://localhost:4568',
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  table: syncConfig.table
 });

test('init', util.init);
const sync = Sync(syncConfig, dynoClient, kinesisClient);

sync.init(function () { // init table
  sync.shards();
  sync.events.on('shardSync', (state) => {
    console.log(`shards have synced, state is ${JSON.stringify(state.stream)}`);
    startInsert();
    startPulling();
    startPullingOther();
  })
});

/***
 * Create a simple data producer that will throw records into a single shard. Only start inserting data once base
 * resources have started.
 */

function startInsert() {
  let payload = Math.round(Math.random() * 100) + ' bottles of beer...';
  let records = [{
    Data: payload,
    PartitionKey: 'we only got 1 shard, so this doesn\'t matter',
  }];
  kinesisClient.putRecords({
    StreamName: syncConfig.streamName,
    Records: records
  }, function (err, result) {
    if (err) throw  err;
    // uncomment the next line to see data getting put to kinesis
    // console.log(`PutRecord result is ${JSON.stringify(result)}`);
    let delay = Math.random() * 1000 + 500;
    setTimeout(startInsert, delay);
  });
}