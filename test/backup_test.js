suite('backup', () => {
  let os = require('os');
  let path = require('path');
  let assert = require('assert');
  let zstd = require('node-zstd');
  let _ = require('lodash');
  let mocks = require('./mocks');
  let Monitor = require('taskcluster-lib-monitor');
  let backup = require('../lib/backup');

  let auth, s3, azure, monitor;
  let bucket = 'foo-backup';

  let defaultEntities = {
    abc: {
      def: [{a: 'b\n'}, {c: 'd'}, {e: 'f'}],
      fed: [{q: 5}, {y: 30.2}],
      qed: [],
    },
    aaa: {
      bbb: [{t: 't'}, {tt: 'tt'}, {ttt: 'ttt'}],
    },
  };

  suiteSetup(async () => {
    s3 = new mocks.s3();
    auth = new mocks.auth();
    azure = mocks.azure;
    monitor = await Monitor({
      project: 'taskcluster-backups',
      patchGlobal: false,
      mock: true,
    });
  });

  async function backupTest({entities, include, ignore, shoulds}) {
    azure.resetEntities();
    _.forEach(entities, (tables, accountId) => {
      _.forEach(tables, (rows, tableName) => {
        azure.setEntities(accountId, tableName, rows);
      });
    });
    await backup.run({
      auth,
      s3,
      azure,
      bucket,
      ignore,
      include,
      concurrency: 10,
      monitor,
    });
    _.forEach(shoulds, should => {
      let results = zstd.decompressSync(s3.things[bucket + should]).toString().split('\n');
      results = _.without(results, ''); // We end up with an extra empty line at the end
      let [accountId, tableName] = should.split('/');
      assert.equal(results.length, entities[accountId][tableName].length);
      results.forEach((res, index) => {
        assert.deepEqual(entities[accountId][tableName][index], JSON.parse(res));
      });
    });
  }

  test('all', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: [], tables: []},
      ignore: {accounts: [], tables: []},
      shoulds: ['abc/def', 'abc/fed', 'abc/qed', 'aaa/bbb'],
    });
  });

  test('includes account', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['aaa'], tables: []},
      ignore: {accounts: [], tables: []},
      shoulds: ['aaa/bbb'],
    });
  });

  test('includes account + table', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['abc'], tables: ['abc/qed']},
      ignore: {accounts: [], tables: []},
      shoulds: ['abc/qed'],
    });
  });

  test('includes account + tables', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['abc'], tables: ['abc/qed', 'abc/def']},
      ignore: {accounts: [], tables: []},
      shoulds: ['abc/qed', 'abc/def'],
    });
  });

  test('includes accounts', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['abc', 'aaa'], tables: []},
      ignore: {accounts: [], tables: []},
      shoulds: ['abc/def', 'abc/fed', 'abc/qed', 'aaa/bbb'],
    });
  });

  test('includes accounts + tables', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['abc', 'aaa'], tables: ['abc/qed', 'aaa/bbb']},
      ignore: {accounts: [], tables: []},
      shoulds: ['abc/qed', 'aaa/bbb'],
    });
  });

  test('ignores account', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: [], tables: []},
      ignore: {accounts: ['abc'], tables: []},
      shoulds: ['aaa/bbb'],
    });
  });

  test('ignores account + table', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: [], tables: []},
      ignore: {accounts: ['aaa'], tables: ['abc/def']},
      shoulds: ['abc/qed', 'abc/fed'],
    });
  });

  test('ignores account + tables', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: [], tables: []},
      ignore: {accounts: ['aaa'], tables: ['abc/def', 'abc/qed']},
      shoulds: ['abc/fed'],
    });
  });

  test('ignores accounts', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: [], tables: []},
      ignore: {accounts: ['abc', 'aaa'], tables: []},
      shoulds: [],
    });
  });

  test('ignores + includes', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['abc'], tables: []},
      ignore: {accounts: [], tables: ['abc/fed']},
      shoulds: ['abc/def', 'abc/qed'],
    });
  });

  test('nonexistent table ignored', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: [], tables: []},
      ignore: {accounts: ['foobarbaz'], tables: []},
      shoulds: [],
    }).then(_ => assert(false, 'Did not throw error when bad ignore.')).catch(err => {
      return assert(err.message.trim().startsWith('Ignored acccounts ["foobarbaz"] are not in set ["abc","aaa"]'));
    });
  });

  test('big tables', async function() {
    let bigEntities = {
      foo: {
        bar: _.range(1004).map(i => ({baz: `bing-${i}`})),
      },
    };
    return backupTest({
      entities: bigEntities,
      include: {accounts: ['foo'], tables: ['foo/bar']},
      ignore: {accounts: [], tables: []},
      shoulds: ['foo/bar'],
    });
  });
});
