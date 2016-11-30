var AWS = require('aws-sdk')
var queue = require('queue-async');
var kinesis = new AWS.Kinesis({region: 'us-east-1'})


if(process.argv[2] === 'attimestamp'){

  var startTime = new Date(process.argv[4]);
  var endTime = new Date(+startTime + 30* 60*1000)
  //if (process.argv[5]) endTime = new Date(process.argv[5])
  console.error(startTime, endTime)
  readKinesisStream(process.argv[3], startTime, endTime, function(err, data) {
    if (err) console.error(err)
    console.log(JSON.stringify(data))
  });
}

if(process.argv[2] === 'ago'){

  var startTime = new Date(+new Date() - parseInt(process.argv[4] * 1000*60));
  var endTime = new Date()

  readKinesisStream(process.argv[3], startTime, endTime, function(err, data) {
    if (err) console.error(err)
    console.log(JSON.stringify(data))
  });
}


function readKinesisStream(stream, startTimestamp, endTimestamp, callback) {
  console.error(startTimestamp.toISOString(), endTimestamp.toISOString())
  kinesis.describeStream({ StreamName: stream }, function(err, result) {
      if (err) return callback(err);
      var q = queue();

      result.StreamDescription.Shards.forEach(function(s) {
          // todo check that shard is active. even if we dont check, we just wont get data from it.
          var getShardParams = {
              ShardId: s.ShardId,
              ShardIteratorType: 'AT_TIMESTAMP',
              StreamName: stream,
              Timestamp: startTimestamp.toISOString()
          };
          q.defer(kinesis.getShardIterator.bind(kinesis), getShardParams);
      });

      q.awaitAll(function(err, iterators) {
          if (err) return callback(err);
          var shardQueue = queue();
          iterators.forEach(function(i) {
              shardQueue.defer(getDataFromShard, i.ShardIterator, []);
          });
          shardQueue.awaitAll(function(err, data) {
              if (err) return callback(err);
              callback(null, data.reduce(function(m, d) {
                  if (d) m.push.apply(m, d);
                  return m;
              }, []));
          });
      });
  });

  function getDataFromShard(iterator, records, cb) {
      var params = {
          ShardIterator: iterator,
          Limit: 10000
      };
      kinesis.getRecords(params, function(err, data) {
        console.error(err, data.Records, data.MillisBehindLatest)
          if (err) return cb(err);
          if (data.Records.length > 0) records.push.apply(records, unpack(data.Records));

          if ((data.Records[0] && new Date(data.Records[0].ApproximateArrivalTimestamp) > new Date(endTimestamp)) || !data.NextShardIterator) {
              // no data, no next iterator or we have read far enough. Time to return the data.
              return cb(null, records);
          }
          // get more data from the stream
          return getDataFromShard(data.NextShardIterator, records, cb);
      });
  }
}

function unpack(records) {
  return records.map(function(r){
    console.log(new Buffer(r.Data, 'base64').toString('utf8'))
    return ;
  })
}
