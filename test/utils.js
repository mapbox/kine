'use strict';

const Dynalite = require('dynalite');
const dynalite = Dynalite({createTableMs: 0});
const queue = require('d3-queue').queue;
const AWS = require('aws-sdk');
const Kinesalite = require('kinesalite');

process.env.AWS_REGION = 'us-east-1';
process.env.AWS_ACCESS_KEY_ID = 'does not matter';
process.env.AWS_SECRET_ACCESS_KEY = 'does not matter';

const config = {
  table: 'kine-kcl-test',
  streamName: 'teststream'
};

const kinesisOptions = {
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  endpoint: 'http://localhost:5568'
};

const kinesisClient = new AWS.Kinesis(kinesisOptions);
const kinesalite = Kinesalite({createStreamMs: 0, shardLimit: 100});

module.exports.init = function (t) {
  const q = queue(1);
  q.defer(dynalite.listen.bind(dynalite), 4568);
  q.defer(kinesalite.listen.bind(kinesalite), 5568);
  q.defer(kinesisClient.createStream.bind(kinesisClient), {
    ShardCount: process.env.shardCount || 2,
    StreamName: config.streamName
  });
  q.await((err) => {
    if (err && err.code != 'ResourceInUseException') t.fail(err.stack);
    console.log(`Dynalite started on port 4568, Kinesalite started on port 5568, created stream ${config.streamName}`);
    t.end();
  });
};

module.exports.teardown = function (t) {
  const q = queue(1);
  q.defer(kinesalite.close);
  q.defer(dynalite.close);
  q.await((err) => {
    if (err) t.fail(err);
    console.log('Kinesalite closed, dynalite closed');
    t.end();
  });
};

module.exports.resetState = function (sync, dynoClient) {
  return new Promise(function (resolve, reject) {
    sync.getShardList(function (err, shards) {
      if (err) return reject(err);
      const q = queue(10);
      shards.forEach(function (shard) {
        q.defer(dynoClient.updateItem, {
          Key: {type: 'shard', id: shard.id},
          ConditionalOperator: 'OR',
          AttributeUpdates: {
            status: {
              Action: 'PUT',
              Value: 'available'
            },
            updated: {
              Action: 'PUT',
              Value: +new Date()
            },
            instance: {
              Action: 'PUT',
              Value: 'myinstance'
            },
            expiresAt: {
              Action: 'PUT',
              Value: +new Date()
            }
          },
          ReturnValues: 'ALL_NEW'
        });
      });
      q.awaitAll(function (err, result) {
        if (err) return reject(err);
        resolve(result);
      });
    });
  });
};