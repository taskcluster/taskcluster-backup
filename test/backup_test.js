suite('backup', () => {
  let os = require('os');
  let path = require('path');
  let assert = require('assert');
  let zstd = require('node-zstd');
  let _ = require('lodash');
  let mocks = require('./mocks');
  let backup = require('../lib/backup');

  let auth, s3, azure;
  let bucket = 'foo-backup';

  suiteSetup(async () => {
    s3 = new mocks.s3();
    auth = new mocks.auth();
    azure = mocks.azure;
  });

  test('basic', async function() {
    let things = [{a: 'b\n'}, {c: 'd'}, {e: 'f'}];
    azure.setEntities(things);
    await backup.run({
      auth,
      s3,
      azure,
      bucket,
      ignore: {accounts: [], tables: []},
      include: {accounts: ['abc'], tables: ['abc/def']},
      concurrency: 1,
    });
    let results = zstd.decompressSync(s3.things[bucket + 'abc/def']).toString().split('\n');
    assert.equal(results.length, 4); // We end up with an extra empty line at the end
    results.forEach((res, index) => {
      if (res) {
        assert.deepEqual(things[index], JSON.parse(res));
      }
    });
  });
});
