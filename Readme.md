# Kine [![Build Status](https://travis-ci.org/mapbox/kine.svg?branch=master)](https://travis-ci.org/mapbox/kine)

Kine makes reading from an aws kinesis stream easy.

Features:

 - [Kinesis Client Library]() like functionality:
  - coordination between multiple instances
  - reads from all available shards
  - start by passing in `init` and `processRecords` callbacks
  - checkpointing in dynamo


#### Publishing a release

Releases are published to npm via GitHub Actions using OIDC trusted publishing — no npm tokens required.

**One-time setup** (already done for this repo — see [PSA doc](https://mapbox.atlassian.net/wiki/spaces/CLOUDPLAT/pages/2894397444)):
1. The `npm-release` GitHub Environment must exist in repo settings, restricted to `master` with required reviewers.
2. The `.github/workflows/npm-release.yml` workflow must be present.

**Cutting a release:**
1. Bump the version in `package.json` (follow semver) and update `CHANGELOG.md` if applicable.
2. Open a PR, get it reviewed, and merge to `master`.
3. Go to **Actions → NPM release → Run workflow**.
4. Approve the `npm-release` environment gate when prompted.

The workflow publishes to npm and creates a GitHub release automatically.

Questions? Reach out in **#ask-platform** on Slack.

---

#### How to use

See [API.md](API.md) for complete reference.

```js
var Kine = require('kine');

var kcl = Kine({
  region: 'us-east-1',
  streamName: 'teststream',
  shardIteratorType: 'TRIM_HORIZON',
  table: 'teststream-kine',
  init: function(done) {
    // do initial setup, context `this` will also be available in processRecords
    console.log(this.id) // `this.id` is the shardId
    done();
  },
  processRecords: function(records, done) {
    // records is an array of records from kinesis.
    console.log(records.length);
    console.log(this.id);  // `this.id` is the shardId.

    // done(err) will throw. Restart with a process manager like upstart
    // done(null, false) with fetch more of the Kinesis stream and not checkpoint
    // done(null, true) will checkpoint, then fetch more off the Kinesis stream.
    done(null, true);
  }
});


```

##### kcl.stop

A kine instance can be halted using `stop`. This will cause any future events to bail out and
remove internal timers

```js
var Kine = require('kine');

var kcl = Kine(/* config */);

kcl.stop();
```

##### kcl.instanceInfo

An instance can be queried by record Partition Key. This allows applications to locate which
shard and instance are responsible for particular records in the stream.

```js
var Kine = require('kine');

var kcl = Kine(/* config */);

kcl.instanceInfo('0230102', function (err, info) {
  // info contains shardId, instance, hashKeyStart and hashKeyEnd
  // for the shard that contains records with partition key '0230102'
});
```
