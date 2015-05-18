var queue = require('queue-async');
var _ = require('lodash');

module.exports = function(dyno, kinesis, config) {
  var db = {};

  db.updateShards = function(cb) {
      kinesis.describeStream({StreamName: config.streamName}, function(err, stream) {
        if(err) throw err;
        var q = queue();

        stream.StreamDescription.Shards.forEach(function(shard) {
          q.defer(dyno.updateItem,
              {type: 'shard', id: shard.ShardId},
              {put: {status: 'available', updated: +new Date(), expiresAt: 0}},
              {expected: {id: {NULL: []}}});
        });

        // look at the list in dynamo, remove any that arent in stream description.
        // maybe possible to do this when getRecords fails because the shard isnt there?
        q.awaitAll(function(err, resp) {
          if(err && err.code !== 'ConditionalCheckFailedException') return cb(err);
          cb(null, stream.StreamDescription.Shards);
        });

      });
  };

  db.availableShard = function(cb) {
    dyno.query({type:{EQ:'shard'}}, function(err, shardsFromDynamo) {
      if(err) return cb(err);
      for(var s=0; s < shardsFromDynamo.length; s++) {
        if ((shardsFromDynamo[s].status === 'available') || (shardsFromDynamo[s].expiresAt < +new Date())) {
          return cb(null, shardsFromDynamo[s]);
        }
      }
      cb(null, null);
    });
  }

  db.leaseShard = function(id, cb) {
    dyno.updateItem(
      {type: 'shard', id: id},
      {put: {
        status: 'leased',
        updated: +new Date(),
        instance: config.instanceId,
        expiresAt: +new Date() + (1000 *20)
      }},
      {
        conditionalOperator: 'OR',
        expected: {
          status: {EQ: 'available'},
          expiresAt: {LE: +new Date()}
        },
      },
      cb);
  }

  db.updateLease = function(s, cb){
    dyno.updateItem(
      {type:'shard', id: s.id},
      {add: {counter: 1}, put: {expiresAt: +new Date() + (1000*20), updated: +new Date()}},
      {
        conidtionalOperator: 'AND',
        expected: {
          expiresAt: {GE: +new Date()},
          status: {EQ: 'leased'},
          instance: {EQ: config.instanceId}
        }
      }, cb);
  };

  db.updateInstance = function(instance, cb) {
    dyno.updateItem(
      {type:'instance', id: config.instanceId},
      {add: {counter: 1}, put: {expiresAt: +new Date() + (1000*20)}},
      cb
    );
  }

  db.cleanupInstances = function(cb) {
    dyno.query({type:{EQ:'instance'}}, {pages:0}, function(err, instances){
      if (err) throw err;
      var q = queue();
      var deleteInstances = [];
      instances.forEach(function(i) {
        if (i.expiresAt < +new Date()) {
          deleteInstances.push(i.id);
          q.defer(dyno.deleteItem, {type:'instance', id: i.id});
        }
      });

      instances = _(instances).filter(function(ins){ return deleteInstances.indexOf(ins.id) == -1;}).value();
      q.awaitAll(function(err, resp) {
        if (err) return cb(err);
        instanceList = instances;
        cb(null, instances);
      });
    });
  }

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
  }
  db.checkpoint = function(shard, cb) {
    dyno.updateItem(
      {type: 'shard', id: shard.id},
      {put: {
        updated: +new Date(),
        expiresAt: +new Date() + (1000 *20),
        checkpoint: shard.sequenceNumber
      }},
      {
        conditionalOperator: 'AND',
        expected: {
          instance: {EQ: config.instanceId },
          expiresAt: {GE: +new Date()-5000}
        }
      },
      cb);
  }
  return db;

};