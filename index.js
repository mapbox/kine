var AWS = require('aws-sdk');
var Kcl = require('./lib/kcl');
var _ = require('lodash');

module.exports = function(config) {
  if(!config) config = {};

  if(!config.streamName) throw new Error('streamName must be configured');

  var kinesisOpts = {
    apiVersion: '2013-12-02',
    region: config.region || 'us-east-1',
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey
  };

  if (config.endpoint && config.endpoint !== '') kinesisOpts.endpoint = new AWS.Endpoint(config.endpoint);
  var kinesis = new AWS.Kinesis(kinesisOpts);

  var kcl = Kcl(config, kinesis);
  kcl.kinesis = kinesis;
  return kcl;
};
