'use strict';

const table = require('./table');
const queue = require('d3-queue').queue;
const _ = require('lodash');

module.exports = function (config, dynoClient, kinesisClient) {
  let db = {};

  /***
   * Create the dynamo table to hold our state tracking. Will be accessed both by our sync workers and kine workers
   * @param cb
   */
  db.createTable = function (cb) {
    dynoClient.createTable(table(config.table), function (err, tableResult) {
      if (err) throw err;
      cb(null, tableResult);
    })
  };

  /***
   * For every shard in a stream description, add an entry to our dynamo table. If it already exists, quietly return
   * anyways. Used mainly by our sync worker.
   * @param cb
   */
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

  /***
   * On every heartbeat from our kcl, update the lease in dynamo. This means pushing further the expiry time and
   * resetting the updated time to now. We also increment the counter by 1.
   * @param s
   * @param cb
   */
  db.updateLease = function (s, cb) {
    dynoClient.updateItem({
      Key: {type: 'shard', id: s.id},
      AttributeUpdates: {
        counter: {
          Action: 'ADD',
          Value: 1
        },
        expiresAt: {
          Action: 'PUT',
          Value: +new Date() + config.leaseTimeout
        },
        updated: {
          Action: 'PUT',
          Value: +new Date()
        }
      },
      ConditionalOperator: 'AND',
      Expected: {
        expiresAt: {
          ComparisonOperator: 'GE',
          AttributeValueList: [+new Date()]
        },
        status: {
          ComparisonOperator: 'EQ',
          AttributeValueList: ['leased']
        },
        instance: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [config.instanceId]
        }
      },
      ReturnValues: 'ALL_NEW'
    }, function (err, response) {
      let updated = err ? response : response.Attributes;
      return cb(err, updated);
    });
  };

  /***
   * Get the shard list from our dynamo table
   * @param cb
   */
  db.getShardList = function (cb) {
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

  /***
   * Get the shard list, and if one of them is either available OR its lease has expired, return it immediately and
   * stop looking.
   * @param cb
   */
  db.availableShard = function (cb) {
    dynoClient.query({
      KeyConditions: {
        type: {
          ComparisonOperator: 'EQ',
          AttributeValueList: ['shard']
        }
      }
    }, function (err, response) {
      if (err) return cb(err);
      const shardsFromDynamo = response.Items;
      for (let s = 0; s < shardsFromDynamo.length; s++) {
        if ((shardsFromDynamo[s].status === 'available') || ((shardsFromDynamo[s].expiresAt < +new Date()) && (shardsFromDynamo[s].status !== 'complete'))) {
          return cb(null, shardsFromDynamo[s]);
        }
      }
      cb(null, null);
    });
  };

  /***
   * Given a shard that is free from availableShard(), try to lease it. We reiterate the conditions in our "expected"
   * clause. We want to be extra sure we won't be leasing 2 shards simultaneously.
   * @param id
   * @param cb
   */
  db.leaseShard = function (id, cb) {
    dynoClient.updateItem({
      Key: {type: 'shard', id: id},
      ConditionalOperator: 'OR',
      AttributeUpdates: {
        status: {
          Action: 'PUT',
          Value: 'leased'
        },
        updated: {
          Action: 'PUT',
          Value: +new Date()
        },
        instance: {
          Action: 'PUT',
          Value: config.instanceId
        },
        expiresAt: {
          Action: 'PUT',
          Value: +new Date() + config.leaseTimeout
        }
      },
      Expected: {
        status: {
          ComparisonOperator: 'EQ',
          AttributeValueList: ['available']
        },
        expiresAt: {
          ComparisonOperator: 'LE',
          AttributeValueList: [+new Date()]
        }
      },
      ReturnValues: 'ALL_NEW'
    }, function (err, response) {
      let updated = err ? response : response.Attributes;
      return cb(err, updated);
    });
  };

  /***
   * If the kcl was requested to checkpoint or called manually, write the sequenceNumber to dynamo for this shard. If
   * our worker dies unexpectedly, we know where to start again.
   * @param shard
   * @param sequenceNumber
   * @param cb
   */
  db.checkpoint = function (shard, sequenceNumber, cb) {
    dynoClient.updateItem({
      Key: {type: 'shard', id: shard.id},
      AttributeUpdates: {
        updated: {
          Action: 'PUT',
          Value: +new Date()
        },
        expiresAt: {
          Action: 'PUT',
          Value: +new Date() + config.leaseTimeout
        },
        checkpoint: {
          Action: 'PUT',
          Value: sequenceNumber || shard.sequenceNumber
        }
      },
      ConditionalOperator: 'AND',
      Expected: {
        instance: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [config.instanceId]
        },
        expiresAt: {
          ComparisonOperator: 'GE',
          AttributeValueList: [+new Date()]
        }
      },
      ReturnValues: 'ALL_NEW'
    }, function (err, response) {
      let updated = err ? response : response.Attributes;
      return cb(err, updated);
    });
  };

  /***
   * Mark a shard as complete.
   * @param s
   * @param cb
   */
  db.shardComplete = function (s, cb) {
    dynoClient.updateItem({
      Key: {type: 'shard', id: s.id},
      AttributeUpdates: {
        status: {
          Action: 'PUT',
          Value: 'complete'
        }
      },
      ConditionalOperator: 'AND',
      Expected: {
        expiresAt: {
          ComparisonOperator: 'GE',
          AttributeValueList: [+new Date()]
        },
        status: {
          ComparisonOperator: 'EQ',
          AttributeValueList: ['leased']
        },
        instance: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [config.instanceId]
        }
      },
      ReturnValues: 'ALL_NEW'
    }, function (err, response) {
      let updated = err ? response : response.Attributes;
      return cb(err, updated);
    });
  };

  return db;
};