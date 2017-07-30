'use strict';

const Db = require('../lib/db2.js');
const EventEmitter = require('events').EventEmitter;
const logger = require('fastlog')('kine');

module.exports = function (config, dynoClient, kinesisClient) {

  let db = Db(config, dynoClient, kinesisClient);

  let sync = {
    events: new EventEmitter(),
    shardSyncDelay: 10000,
    verbose: config.verbose || false
  };

  sync.init = function (cb) {
    db.createTable((err, tableResult) => {
      if (err) return cb(err);
      sync.emitEvent('syncInit', tableResult);
      cb(null, tableResult);
    });
  };

  sync.shards = function () {
    db.updateShards((err, state) => {
      if (err) throw err;
      sync.shardSyncTimeout = setTimeout(sync.shards, sync.shardSyncDelay); // keep a ref to the timeout so we can cancel it if needed
      sync.emitEvent('shardSync', state);
    })
  };

  sync.getShardList = function(cb){
    db.getShardList((err, result) => {
      if(err) throw err;
      sync.emitEvent('getShardList', result);
      cb(null, result);
    })
  };

  sync.emitEvent = function(event, data){
    if(sync.verbose) logger.info(`[ Sync ] ${event}`);
    sync.events.emit(event, data);
  };

  return sync;

};