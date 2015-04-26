var kinesis = require('kinesis-readable');
var stream = require('stream');

// required config:
// - name: of the stream
// - region: the stream is in
//
// optional config:
// - shardId: that you want to read from
// - limit: the number of records fed to your function
// - latest: if you want to start reading only new records
// - lastCheckpoint: if you want to start reading right after some record
// - endpoint: for Kinesis
// - accessKeyId: if you want to be explicit about credentials
// - secretAccesKey: if you want to be explicit about credentials
// - sessionToken: if you want to be explicit about credentials

module.exports = function(config, processRecords) {
  var Readable = kinesis(config);

  var readable = new Readable(config);
  var transform = new stream.Transform({ objectMode: true });

  transform._transform = function(records, enc, callback) {
    processRecords(records, function(err, result) {
      transform.push(result);
      callback(err);
    });
  };

  readable.on('error', function(err) {
    transform.emit('error', err);
  });

  transform.close = function(callback) {
    readable.on('end', callback);
    readable.close();
  };

  return readable.pipe(transform);
};
