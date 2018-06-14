'use strict';

/*
 Usage: kine ls myStreamName
 */

const aws = require('aws-sdk');
const kinesis = new aws.Kinesis({region: process.env.AWS_DEFAULT_REGION || 'us-east-1'});
const cloudwatch = new aws.CloudWatch({region: process.env.AWS_DEFAULT_REGION || 'us-east-1'});
const table = require('terminal-table');
const colors = require('colors');

module.exports = function (stream) {

  const shardTable = new table({
    borderStyle: 0,
    horizontalLine: true,
    leftPadding: 1,
    rightPadding: 1,
    border: {
      sep: "║",
      topLeft: "╔", topMid: "╦", top: "═", topRight: "╗",
      midLeft: "╠", midMid: "╬", mid: "═", midRight: "╣",
      botLeft: "╚", botMid: "╩", bot: "═", botRight: "╝"
    }
  });

  function getAverage(shard, enhancedMonitoring, metric, streamName) {
    if (enhancedMonitoring && enhancedMonitoring[0].ShardLevelMetrics) {
      if (enhancedMonitoring[0].ShardLevelMetrics.indexOf(metric) != -1) {
        return cloudwatch.getMetricStatistics({
          MetricName: metric,
          Namespace: 'AWS/Kinesis',
          Period: 60 * 60,
          EndTime: new Date(+new Date() - (60 * 1000)),
          StartTime: new Date(+new Date() - (60 * 1000 * 60)),
          Dimensions: [
            {
              Name: 'StreamName',
              Value: streamName
            },
            {
              Name: 'ShardId',
              Value: shard.ShardId
            }
          ],
          Statistics: ['Sum']
        }).promise().then((result) => {
          if (result.Datapoints.length === 0) return 'n/a';
          return Math.round(result.Datapoints[0].Sum);
        })
      }
    }
    return Promise.resolve('n/a');
  }

  let tableItems = [];
  shardTable.push(['ShardId'.yellow, 'ParentShardId'.magenta, 'Avg. in (last hour)'.green, 'Avg. out (last hour)'.cyan]);

  kinesis.describeStream({StreamName: stream}).promise()
    .then((result) => {
      console.log(`\n${result.StreamDescription.StreamARN.bold}\n`);
      let avgIn = [];
      let avgOut = [];
      for (let i = 0; i < result.StreamDescription.Shards.length; i++) {
        let shard = result.StreamDescription.Shards[i];
        tableItems.push([shard.ShardId.yellow, shard.ParentShardId.magenta]);
        // queue up all the promises for extended stats. We're not sure yet if they are enabled here
        avgIn.push(getAverage(shard, result.StreamDescription.EnhancedMonitoring, 'IncomingBytes', stream));
        avgOut.push(getAverage(shard, result.StreamDescription.EnhancedMonitoring, 'OutgoingBytes', stream));
      }
      return [avgIn, avgOut];
    })
    .then((inOut) => Promise.all([Promise.all(inOut[0]), Promise.all(inOut[1])])) // resolve all our promises
    .then((result) => {
      // add the extra columns if there is data for them
      result.forEach((item, pairIndex) => {
        item.forEach((v, i) => {
          if (v === 'n/a') {
            v = !pairIndex ? v.green : v.cyan;
            tableItems[i].push(v);
          } else {
            let value = String((v / (1000000 * 60)).toFixed(2) + ' MB/minute');
            value = !pairIndex ? value.green : value.cyan;
            tableItems[i].push(value)
          }
        });
      });
    })
    .then(() => {
      // for each array in our array, add to the table
      tableItems.map(r => shardTable.push(r));
      console.log('' + shardTable); // render the thing
    })
    .catch((err) => {
      console.log(err.stack || err);
    });
};
