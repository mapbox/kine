# Scuba

Explore the depths of your AWS kinesis streams.

## Available commands

- `ls` : show information about a kinesis stream
- `pull` : get records from a kinesis stream

## Setup

```bash
TBD
```

**Important!** Scuba assumes your shell has proper AWS credentials configured to access resources in your account. At a minimum, you should have

```
AWS_ACCESS_KEY_ID=XXXXX
AWS_SECRET_ACCESS_KEY=YYYYYY
AWS_DEFAULT_REGION=us-east-1
```

## Usage

### ls

Display information about a kinesis stream and its shards.

Parameters:

- `stream name`
  - description: the name of your kinesis stream
  - required: yes
  - default: n/a

#### Examples

```
scuba ls StreamName
```

![screen shot 2017-07-08 at 10 55 41 am](https://user-images.githubusercontent.com/1378612/27987974-1334aebe-63cc-11e7-8a2a-86ec6834d3b8.png)

Note: Cloudwatch stats will only show if you have enabled [shard-level metrics](https://docs.aws.amazon.com/streams/latest/dev/monitoring-with-cloudwatch.html#kinesis-metrics-shard) on your stream.

<hr />

### pull

Get records for one or many shards in a stream

Parameters:

- `stream name`
  - description: the name of your kinesis stream
  - required: yes
  - default: n/a
- `-t`
  - description: the type of shard iterator. Can be one of `LATEST`, `TRIM_HORIZON` or `AT_TIMESTAMP`
  - required: no
  - default: `LATEST`  
- `-d`
  - description: if `-t` is `AT_TIMESTAMP`, the timestamp to start reading at
  - required: yes, if `-t` is `AT_TIMESTAMP`. Otherwise, no.
  - default: n/a
- `-i`
  - description: specify a shard id to read from
  - required: no
  - default: scuba will read from all shards if `-i` is not present
- `-o`
  - description: create a folder to write the result. Inside this folder, every shard will have a file named after its shard id.
  - required: no
  - default: output goes to stdout
- `-z`
  - description: how fast should be pull from shards on a stream (1, 2 or 3 second timeout). Can be one of `slow`, `normal` or `fast`
  - required: no
  - default: `normal`

#### Examples

```
scuba pull StreamName
```
- `LATEST` shardIterator type
- pull from all shards at once
- reads at `normal` speed
- outputs to stdout

```
scuba pull StreamName -i shardId-000000000001
```
- same as above, but pull from shard `shardId-000000000001` only

```
scuba pull StreamName -t TRIM_HORIZON -z fast -i shardId-000000000001
```
- `TRIM_HORIZON` shardIterator type
- wait only 1 second between pulls on each shard
- outputs to stdout

```
scuba pull StreamName -t AT_TIMESTAMP -d 1499476500000
```
- start reading at timestamp `1499476500000`

```
scuba pull StreamName -t TRIM_HORIZON -i shardId-000000000001 -o output

Found 0 records in shardId-000000000001
Found 1 records in shardId-000000000001. ms behind is 86399000
Found 501 records in shardId-000000000001. ms behind is 86382000
Found 1000 records in shardId-000000000001. ms behind is 86371000
Found 1000 records in shardId-000000000001. ms behind is 86221000
Found 1000 records in shardId-000000000001. ms behind is 86217000
Found 500 records in shardId-000000000001. ms behind is 86221000
...
$ ls -lah output
$ .
$ ..
$ 27M shardId-000000000001.json
```
- `TRIM_HORIZON` shardIterator type
- pull from shard `shardId-000000000001` only
- create a folder called `output`, containing a single file called `shardId-000000000001.json`

This command will also provide stats to stdout on current progress while writing to folder.
