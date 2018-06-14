'use strict';

/*
 Usage: kine pull myStreamName -t LATEST/TRIM_HORIZON/AT_TIMESTAMP -d 1499408313497 -i shard-00000000001 -o folderName
 -t: shardIteratorType. Will default to LATEST
 -d: if shardIteratorType === 'AT_TIMESTAMP', timestamp to start reading at
 -i: specify a shardId – otherwise kine will read from all shards at once
 -o: specify a folder name to create, data from each shard will output to a file named after the shard. If not specified, data goes to stdout
 -z: how fast should we read from the stream (delay time)? accepted values are: "slow" (3000ms), "normal" (2000ms) or "fast" (1000ms). Defaults to "normal" (2000ms)
 */

const aws = require('aws-sdk');
const kinesis = new aws.Kinesis({region: process.env.AWS_DEFAULT_REGION || 'us-east-1'});
const limit = 10000;
const fs = require('graceful-fs');

let writeHandles = {};

function determineShardIteratorType(args) {
  if ('t' in args) {
    if (args.t === 'LATEST' || args.t === 'TRIM_HORIZON' || args.t === 'AT_TIMESTAMP') {
      if (args.t === 'AT_TIMESTAMP') {
        if (!('d' in args)) {
          throw new Error('-t AT_TIMESTAMP needs to specify a -d parameter');
        }
        return args.t;
      }
      return args.t;
    }
  }
  return 'LATEST';
}

function getDelay(args) {
  if ('z' in args) {
    if (args.z === 'slow' || args.z === 'normal' || args.z === 'fast') {
      if (args.z === 'slow') return 3000;
      if (args.z === 'normal') return 2000;
      if (args.z === 'fast') return 1000;
    } else {
      throw new Error('bad value (accepted values are normal, slow or fast');
    }
  }
  return 2000;
}

function getRecords(params, shardId, args, delay) {
  return kinesis.getRecords(params).promise().then((r) => {
    if (args.o) {
      let msBehind = r.Records.length ? '. ms behind is ' + r.MillisBehindLatest : '';
      console.log(`Found ${r.Records.length} records in ${shardId}${msBehind}`);
    }
    for (let a = 0; a < r.Records.length; a++) {
      if (args.o) {
        writeHandles[shardId].write(new Buffer(r.Records[a].Data).toString() + '\n');
      } else {
        console.log(new Buffer(r.Records[a].Data).toString());
      }
    }
    if (r.NextShardIterator) {
      setTimeout(function () {
        return getRecords({
          ShardIterator: r.NextShardIterator,
          Limit: limit
        }, shardId, args, delay)
      }, delay);
    }
  })
}

module.exports = function (args) {
  let stream = args._[1];
  let shardIds = [];
  let delay = getDelay(args);

  if ('o' in args) {
    if (!fs.existsSync(args.o)) {
      fs.mkdirSync(args.o);
    } else {
      console.log(`folder already exists, appending to current files`);
    }
  }

  kinesis.describeStream({StreamName: stream}).promise()
    .then((result) => {
      let iteratorType = determineShardIteratorType(args);
      let ts;
      if (iteratorType === 'AT_TIMESTAMP') {
        try {
          ts = new Date(args.d).toISOString();
        } catch (e) {
          throw new Error(e);
        }
      }
      let getShardIteratorOps = [];
      let params;
      if (args.i) {
        params = {
          ShardId: args.i,
          ShardIteratorType: iteratorType,
          StreamName: stream
        };
        shardIds.push(args.i);
        if (args.o) writeHandles[args.i] = fs.createWriteStream(`${args.o}/${args.i}.json`, {'flags': 'a'});
        getShardIteratorOps.push(kinesis.getShardIterator(params).promise());
      } else {
        for (let i = 0; i < result.StreamDescription.Shards.length; i++) {
          let shard = result.StreamDescription.Shards[i];
          shardIds.push(shard.ShardId);
          if (args.o) writeHandles[shard.ShardId] = fs.createWriteStream(`${args.o}/${shard.ShardId}.json`, {'flags': 'a'});
          params = {
            ShardId: shard.ShardId,
            ShardIteratorType: iteratorType,
            StreamName: stream
          };
          if (ts) params.Timestamp = ts;
          getShardIteratorOps.push(kinesis.getShardIterator(params).promise());
        }
      }
      return getShardIteratorOps;
    })
    .then(Promise.all.bind(Promise))
    .then((shardIterators) => {
      let getRecordsOps = [];
      for (let i = 0; i < shardIterators.length; i++) {
        let params = {
          ShardIterator: shardIterators[i].ShardIterator,
          Limit: limit
        };
        getRecordsOps.push(getRecords(params, shardIds[i], args, delay));
      }
      return Promise.all(getRecordsOps);
    })
    .catch((err) => console.log(err.stack || err));
};
