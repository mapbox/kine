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
    -   `options.streamName` **string** the name of the kinesis stream to consume
    -   `options.endpoint` **[string]** the kinesis endpoint url
    -   `options.dynamoEndpoint` **[string]** the dynamodb endpoint url
    -   `options.sessionToken` **[string]** credentials for the client to utilize
    -   `options.accessKeyId` **[string]** credentials for the client to utilize
    -   `options.secretAccessKey` **[string]** credentials for the client to utilize
    -   `options.sessionToken` **[string]** credentials for the client to utilize
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
