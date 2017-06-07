# index

Creates a kine client. You must provide a stream name and the region where the
stream resides. You can also provide the name of the dynamodb table that kine will
use for tracking shard leases and checkpointing progess.

If you do not explicitly pass credentials when creating a kine client, the
aws-sdk will look for credentials in a variety of places. See [the configuration guide](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html)
for details.

**Parameters**

-   `options` **object** configuration parameters
    -   `options.region` **string** the region in which the default stream resides
    -   `options.shardIteratorType` **string** where to start in the stream. `LATEST` or `TRIM_HORIZON`
    -   `options.table` **string** the dynamodb table to use for tracking shard leases.
    -   `options.init` **function** function that is called when a new lease of a shard is started
    -   `options.processRecords` **function** function is that called when new records are fetches from the kinesis shard.
    -   `options.maxShards` **[string]** max number of shards to track per process. defaults to 10
    -   `options.limit` **[string]** limit used for requests to kinesis for the number of records.  This is a max, you might get few records on process recirds
    -   `options.streamName` **string** the name of the kinesis stream to consume
    -   `options.endpoint` **[string]** the kinesis endpoint url
    -   `options.dynamoEndpoint` **[string]** the dynamodb endpoint url
    -   `options.accessKeyId` **[string]** credentials for the client to utilize
    -   `options.secretAccessKey` **[string]** credentials for the client to utilize
    -   `options.sessionToken` **[string]** credentials for the client to utilize
    -   `options.credentials` **[AWS.Credentials]** [AWS.Credentials](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Credentials.html) object for the client to utilize
    -   `options.cloudwatchNamespace` **[string]** namespace to use for custom cloudwatch reporting of shard ages. required if `cloudwatchStackname` is set
    -   `options.cloudwatchStackname` **[string]** stack name to use as a dimension on custom cloudwatch reporting of shard ages. required if `cloudwatchNamespace` is set
    -   `options.maxProcessTime` **[string]** max number of millseconds between getting records before considering a process a zombie . defaults to 300000 (5mins)
-   `config`  

**Examples**

```javascript
var Kine = require('kine');
var kine = Kine({
  streamName: 'my-kinesis-stream',
  region: 'us-east-1'
});
```

Returns **client** a kine client
