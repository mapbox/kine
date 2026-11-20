var queue = require('d3-queue').queue;
var _ = require('lodash');

module.exports = function(dyno, kinesis, config) {
  var db = {};

  // List all shards of the stream. DescribeStream returns at most 100 shards per call,
  // and closed shards stay listed for 24 hours after a reshard, so follow HasMoreShards.
  db.listAllShards = function(cb) {
    var shards = [];
    (function next(exclusiveStartShardId) {
      var params = {StreamName: config.streamName};
      if (exclusiveStartShardId) params.ExclusiveStartShardId = exclusiveStartShardId;
      kinesis.describeStream(params, function(err, stream) {
        if (err) return cb(err);
        var page = stream.StreamDescription.Shards;
        shards = shards.concat(page);
        if (stream.StreamDescription.HasMoreShards && page.length) return next(page[page.length - 1].ShardId);
        cb(null, shards);
      });
    })();
  };

  // Create a lease row for each shard that does not have one yet.
  // A row that already exists fails the condition with ConditionalCheckFailedException. That is expected,
  // so it must not fail the queue: d3-queue aborts all other in-flight requests on the first error,
  // which after a reshard left most new shards without a lease row.
  function createShardRow(shard, cb) {
    dyno.updateItem({
      Key: { type: 'shard', id: shard.ShardId},
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
    }, function(err) {
      if (err && err.code !== 'ConditionalCheckFailedException') return cb(err);
      cb();
    });
  }

  db.updateShards = function(cb) {
    db.listAllShards(function(err, shards) {
      if (err) return cb(err);
      var q = queue();
      shards.forEach(function(shard) {
        q.defer(createShardRow, shard);
      });
      q.awaitAll(function(err) {
        if (err) return cb(err);
        cb(null, shards);
      });
    });
  };

  db.getShardList = function(cb) {
    dyno.query({
      KeyConditions: {
        type: {
          ComparisonOperator: 'EQ',
          AttributeValueList: ['shard']
        }
      }
    }, function (err, response) {
      var shards = err ? response : response.Items;
      return cb(err, shards);
    });
  };

  db.availableShard = function(cb) {
    dyno.query({
      KeyConditions: {
        type: {
          ComparisonOperator: 'EQ',
          AttributeValueList: ['shard']
        }
      }
    }, function(err, response) {
      if(err) return cb(err);
      var shardsFromDynamo = response.Items;
      for(var s=0; s < shardsFromDynamo.length; s++) {
        if ((shardsFromDynamo[s].status === 'available') || ((shardsFromDynamo[s].expiresAt < +new Date()) && (shardsFromDynamo[s].status !== 'complete'))) {
          return cb(null, shardsFromDynamo[s]);
        }
      }
      cb(null, null);
    });
  };

  db.leaseShard = function(id, cb) {
    dyno.updateItem({
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
          Value: +new Date() + config._leaseTimeout
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
      var updated = err ? response : response.Attributes;
      return cb(err, updated);
    });
  };

  db.updateLease = function(s, cb){
    dyno.updateItem({
      Key: {type:'shard', id: s.id},
      AttributeUpdates: {
        counter: {
          Action: 'ADD',
          Value: 1
        },
        expiresAt: {
          Action: 'PUT',
          Value: +new Date() + config._leaseTimeout
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
      var updated = err ? response : response.Attributes;
      return cb(err, updated);
    });
  };

  db.shardComplete = function(s, cb) {
    dyno.updateItem({
      Key: {type:'shard', id: s.id},
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
      var updated = err ? response : response.Attributes;
      return cb(err, updated);
    });
  };

  db.updateInstance = function(instance, cb) {
    dyno.updateItem({
      Key: {type:'instance', id: config.instanceId},
      AttributeUpdates: {
        counter: {
          Action: 'ADD',
          Value: 1
        },
        expiresAt: {
          Action: 'PUT',
          Value: +new Date() + config._leaseTimeout
        }
      },
      ReturnValues: 'ALL_NEW'
    }, function (err, response) {
      var updated = err ? response : response.Attributes;
      return cb(err, updated);
    });
  };

  db.cleanupInstances = function(cb) {
    dyno.query({
      KeyConditions: {
        type: {
          ComparisonOperator: 'EQ',
          AttributeValueList: ['instance']
        }
      },
      Pages: Infinity
    }, function(err, response){
      if (err) throw err;
      var q = queue();
      var deleteInstances = [];
      var instances = response.Items;

      instances.forEach(function(i) {
        if (i.expiresAt < +new Date()) {
          deleteInstances.push(i.id);
          q.defer(dyno.deleteItem, {Key: {type:'instance', id: i.id}});
        }
      });

      instances = _(instances).filter(function(ins){ return deleteInstances.indexOf(ins.id) == -1;}).value();
      q.awaitAll(function(err) {
        if (err) return cb(err);
        cb(null, instances);
      });
    });
  };

  db.getIterator = function(shard, cb) {
    var getIteratorOpts = {
      StreamName: config.streamName,
      ShardId: shard.id,
      ShardIteratorType: config.shardIteratorType
    };

    if (shard.checkpoint) {
      getIteratorOpts.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
      getIteratorOpts.StartingSequenceNumber = shard.checkpoint;
    }

    kinesis.getShardIterator(getIteratorOpts,cb);
  };

  db.checkpoint = function(shard, sequenceNumber, cb) {
    if(typeof sequenceNumber === 'function') { // shift params if sequenceNumber is a function
      cb = sequenceNumber;
      sequenceNumber = null;
    }
    dyno.updateItem({
      Key: {type: 'shard', id: shard.id},
      AttributeUpdates: {
        updated: {
          Action: 'PUT',
          Value: +new Date()
        },
        expiresAt: {
          Action: 'PUT',
          Value: +new Date() + config._leaseTimeout
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
      var updated = err ? response : response.Attributes;
      return cb(err, updated);
    });
  };
  return db;
};
