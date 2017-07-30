'use strict';
const os = require('os');
const queue = require('d3-queue').queue;
const Db = require('../lib/db2.js');
const EventEmitter = require('events').EventEmitter;

module.exports = function (config, dynoClient, kinesisClient) {

  let db = Db(config, dynoClient, kinesisClient);

  let kcl = {
    shard: {
      updated: +new Date()
    },
    instanceId: [os.hostname(), process.pid, +new Date()].join('-'), // the current consumer
    events: new EventEmitter()
  };


  // let stop = false;
  //
  // kcl.stop = function () {
  //   stop = true;
  // };

  kcl.init = function(cb){
    // get a lease


  };

  kcl.heartbeat = function () {
    //if (stop) return;
    let q = queue(1);

    // if we haven't called getRecords recently, we consider this process a zombie and shut it down.
    if (shard.updated < (+new Date() - config.maxProcessTime)) {
      throw Error(`Max processing time reached: ${config.maxProcessTime}`);
    }

    if (shard.status !== 'complete') {
      q.defer(db.updateLease, shard);
    }

    q.awaitAll(function (err) {
      if (err) throw err;
      setTimeout(heartbeat, 5000);
    });
  };

  return kcl;
};