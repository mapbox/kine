'use strict';

const Db = require('../lib/db.js');
const EventEmitter = require('events').EventEmitter;
const logger = require('fastlog')('kine');

module.exports = function (config, dynoClient, kinesisClient) {

  let db = Db(config, dynoClient, kinesisClient);

  // TODO cleanup instances that are old

  let sync = {
    events: new EventEmitter(),
    shardSyncDelay: 10000,
    verbose: config.verbose || false
  };

  /***
   * Make sure dynamo table exists before syncing state
   * @param cb
   */
  sync.init = function (cb) {
    db.createTable((err, tableResult) => {
      if (err) return cb(err);
      sync.emitEvent('syncInit', tableResult);
      cb(null, tableResult);
    });
  };

  /***
   * Sync kinesis stream state with dynamo table
   */
  sync.shards = function () {
    db.updateShards((err, state) => {
      if (err) throw err;
      sync.shardSyncTimeout = setTimeout(sync.shards, sync.shardSyncDelay); // keep a ref to the timeout so we can cancel it if needed
      sync.emitEvent('shardSync', state);
    })
  };

  /**
   * Get information about shards of a stream
   * @param cb
   */
  sync.getShardList = function(cb){
    db.getShardList((err, result) => {
      if(err) throw err;
      sync.emitEvent('getShardList', result);
      cb(null, result);
    })
  };

  /***
   * Log and emit events
   * @param event
   * @param data
   */
  sync.emitEvent = function(event, data){
    if(sync.verbose) logger.info(`[ Sync ] ${event}`);
    sync.events.emit(event, data);
  };

  return sync;

};