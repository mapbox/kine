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

module.exports.init = function(t) {
  const q = queue(1);
  q.defer(dynalite.listen.bind(dynalite), 4568);
  q.defer(kinesalite.listen.bind(kinesalite), 5568);
  q.defer(kinesisClient.createStream.bind(kinesisClient), {ShardCount: 4, StreamName: config.streamName});
  q.await((err) => {
    if (err) t.fail(err.stack);
    console.log(`Dynalite started on port 4568, Kinesalite started on port 5568, created stream ${config.streamName}`);
    t.end();
  });
};

module.exports.teardown = function(t) {
  const q = queue(1);
  q.defer(kinesalite.close);
  q.defer(dynalite.close);
  q.await((err) => {
    if (err) t.fail(err);
    console.log('Kinesalite closed, dynalite closed');
    t.end();
  });
};