Kine makes reading from an aws kinesis stream easy.

Goals:

 - kcl like functionality,
  - coordination between multiple instances
  - reads from all available shards
  - start by passing in `init` and `processRecords` callbacks
  - checkpointing in dynamo

WIP, examples coming soon.
