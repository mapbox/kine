var queue = require('queue-async');

module.exports = function(dyno, kinesis) {
  var db = {};

  db.insureTable = function(tableDef, callback) {
    dyno.createTable(tableDef, callback);
  };

  db.addInstance = function(instanceId, callback) {
    var key = { type: 'instance', id: instanceId };
    var update = {
      add: { counter: 1 },
      put: { expiresAt: +new Date() + (1000 * 20) }
    };

    dyno.updateItem(key, update, callback);
  };

  db.listInstances = function(callback) {
    var query = { type: { EQ: 'instance' } };
    var options = { pages: 0 };

    dyno.query(query, options, function(err, instances) {
      if (err) return callback(err);

      var toDelete = [];
      var q = queue();
      instances.forEach(function(instance) {
        if (instance.expiresAt < +new Date()) {
          toDelete.push(instance.id);
          q.defer(dyno.deleteItem, { type: 'instance', id: instance.id });
        }
      });

      instances = instances.filter(function(instance) {
        return toDelete.indexOf(instance.id) == -1;
      });

      q.awaitAll(function(err) {
        if (err) return callback(err);
        callback(null, instances);
      });
    });
  };

  db.heartbeat = function(shardId, callback) {
    var key = { type: 'shard', id: shardId };
    var update = {
      add: { counter: 1 },
      put: {
        expiresAt: +new Date() + (1000 * 20),
        updated: +new Date()
      }
    };
    var options = {
      conditionalOperator: 'AND',
      expected: {
        expiresAt: { GE: +new Date() },
        status: { EQ: 'leased' },
        instance: { EQ: config.instanceId }
      }
    };

    dyno.updateItem(key, update, options, callback);
  };

  db.listShards = function(streamName, callback) {
    kinesis.describeStream({
      StreamName: streamName,
      Limit: opts.limit || 100
    },
    function(err, stream) {
      if (err) return callback(err);

      var q = queue();

      stream.StreamDescription.Shards.forEach(function(shard) {
        var key = { type: 'shard', id: shard.ShardId };
        var update = {
          put: {
            status: 'available',
            updated: +new Date(),
            expiresAt: 0
          }
        };
        var options = { expected: { id: { NULL: [] } } };
        q.defer(dyno.updateItem, key, update, options);
      });

      q.awaitAll(function(err) {
        if (err && err.code !== 'ConditionalCheckFailedException') return callback(err);
        callback(null, stream.StreamDescription.Shards);
      });
    });
  };

  db.lease = function(instanceId, callback) {
    var query = { type: { EQ: 'shard' } };

    dyno.query(query, function(err, shards) {
      if (err) return callback(err);

      var key = { type: 'shard', id: null };
      var update = {
        put: {
          status: 'leased',
          updated: +new Date(),
          instance: instanceId,
          expiresAt: +new Date() + (1000 * 20)
        }
      };
      var options = {
        conditionalOperator: 'AND',
        expected: null
      };
      var shard;

      for (var s = 0; s < shards.length; s++) {
        shard = shards[s];
        key.id = shard.id;

        if (shard.status === 'available') {
          options.expected = { status: { EQ: 'available' } };
        } else if (shard.expiresAt < +new Date()) {
          options.expected = {
            status: { EQ: 'leased' },
            expiresAt: { LE: +new Date() }
          };
        }

        if (options.expected)
          return dyno.updateItem(key, update, options, done);
      }

      callback();

      function done(err, shard) {
        if (err && err.code === 'ConditionalCheckFailedException')
          return setTimeout(db.lease, 1000, instanceId).unref();
        else if (err) return callback(err);

        if (shard.instance === instanceId) return callback(null, shard);
        callback();
      }
    });
  };

  db.checkpoint = function(instanceId, shardId, sequenceNumber, callback) {
    var key = { type: 'shard', id: shardId };
    var update = {
      put: {
        updated: +new Date(),
        expiresAt: +new Date() + (1000 * 20),
        checkpoint: sequenceNumber
      }
    };
    var options = {
      conditionalOperatro: 'AND',
      expected: {
        instance: { EQ: instanceId },
        expiresAt: { GE: +new Date() }
      }
    };

    dyno.updateItem(key, update, options, callback);
  };

  return db;
};
