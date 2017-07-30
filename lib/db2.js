'use strict';

const table = require('./table');
const queue = require('d3-queue').queue;
const _ = require('lodash');

module.exports = function (config, dynoClient, kinesisClient) {
  let db = {};

  db.createTable = function (cb) {
    dynoClient.createTable(table(config.table), function (err, tableResult) {
      if (err) throw err;
      cb(null, tableResult);
    })
  };

  db.updateShards = function (cb) {
    kinesisClient.describeStream({StreamName: config.streamName}, function (err, stream) {
      if (err) return cb(err);
      let q = queue();

      stream.StreamDescription.Shards.forEach(function (shard) {
        q.defer(dynoClient.updateItem, {
          Key: {type: 'shard', id: shard.ShardId},
          AttributeUpdates: {
            status: {
              Action: 'PUT',
              Value: 'available'
            },
            hashKeyStart: {
              Action: 'PUT',
              Value: shard.HashKeyRange.StartingHashKey
            },
            hashKeyEnd: {
              Action: 'PUT',
              Value: shard.HashKeyRange.EndingHashKey
            },
            updated: {
              Action: 'PUT',
              Value: +new Date()
            },
            expiresAt: {
              Action: 'PUT',
              Value: 0
            }
          },
          Expected: {
            id: {
              ComparisonOperator: 'NULL'
            }
          },
          ReturnValues: 'ALL_NEW'
        });
      });

      q.awaitAll(function (err, state) {
        if (err && err.code !== 'ConditionalCheckFailedException') return cb(err);
        cb(null, {stream: stream.StreamDescription.Shards, state});
      });
    });
  };

  db.getShardList = function(cb) {
    dynoClient.query({
      KeyConditions: {
        type: {
          ComparisonOperator: 'EQ',
          AttributeValueList: ['shard']
        }
      }
    }, function (err, response) {
      let shards = err ? response : response.Items;
      return cb(err, shards);
    });
  };

  return db;
};