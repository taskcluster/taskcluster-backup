suite('backup', () => {
  let assert = require('assert');
  let zstd = require('node-zstd');
  let _ = require('lodash');
  let mocks = require('./mocks');
  let Monitor = require('taskcluster-lib-monitor');
  let backup = require('../src/backup');

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

  let defaultContainers = {
    abc: {
      contA: [{name: '1', content: 'aaa1'}, {name: '2', content: 'aaa2'}],
      contB: [{name: '1', content: 'bbb1'}, {name: '2', content: 'bbb2'}],
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

  async function backupTest({entities, containers, include, ignore, shoulds}) {
    azure.resetEntities();
    _.forEach(entities || {}, (tables, accountId) => {
      _.forEach(tables, (rows, tableName) => {
        azure.setEntities(accountId, tableName, rows);
      });
    });
    azure.resetContainers();
    _.forEach(containers || {}, (containers, accountId) => {
      _.forEach(containers, (blobs, containerName) => {
        azure.setContainers(accountId, containerName, blobs);
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
      let key = bucket + should;
      assert(s3.things[key], `${key} not in S3`);
      let results = zstd.decompressSync(s3.things[bucket + should]).toString().split('\n');
      results = _.without(results, ''); // We end up with an extra empty line at the end
      let [accountId, type, name] = should.split('/');
      if (type === 'table') {
        assert.equal(results.length, entities[accountId][name].length);
        results.forEach((res, index) => {
          assert.deepEqual(entities[accountId][name][index], JSON.parse(res));
        });
      } else {
        let expected = containers[accountId][name].map(b => ({info: {content: b.content}, name: b.name}));
        assert.equal(results.length, containers[accountId][name].length);
        assert.deepEqual(results.map(r => JSON.parse(r)), expected);
      }
    });
  }

  test('all', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {},
      ignore: {},
      shoulds: ['abc/table/def', 'abc/table/fed', 'abc/table/qed', 'aaa/table/bbb'],
    });
  });

  test('includes account', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['aaa']},
      ignore: {},
      shoulds: ['aaa/table/bbb'],
    });
  });

  test('includes account + table', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['abc'], tables: ['abc/qed']},
      ignore: {},
      shoulds: ['abc/table/qed'],
    });
  });

  test('includes account + containers', async function() {
    return backupTest({
      entities: defaultEntities,
      containers: defaultContainers,
      include: {accounts: ['abc'], tables: ['abc/qed']},
      ignore: {},
      shoulds: ['abc/table/qed', 'abc/container/contA', 'abc/container/contB'],
    });
  });

  test('includes account + tables', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['abc'], tables: ['abc/qed', 'abc/def']},
      ignore: {},
      shoulds: ['abc/table/qed', 'abc/table/def'],
    });
  });

  test('includes accounts', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['abc', 'aaa']},
      ignore: {},
      shoulds: ['abc/table/def', 'abc/table/fed', 'abc/table/qed', 'aaa/table/bbb'],
    });
  });

  test('includes accounts + tables', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['abc', 'aaa'], tables: ['abc/qed', 'aaa/bbb']},
      ignore: {},
      shoulds: ['abc/table/qed', 'aaa/table/bbb'],
    });
  });

  test('ignores account', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {},
      ignore: {accounts: ['abc']},
      shoulds: ['aaa/table/bbb'],
    });
  });

  test('ignores account + table', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {},
      ignore: {accounts: ['aaa'], tables: ['abc/def']},
      shoulds: ['abc/table/qed', 'abc/table/fed'],
    });
  });

  test('ignores account + tables', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {},
      ignore: {accounts: ['aaa'], tables: ['abc/def', 'abc/qed']},
      shoulds: ['abc/table/fed'],
    });
  });

  test('ignores accounts', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {},
      ignore: {accounts: ['abc', 'aaa']},
      shoulds: [],
    });
  });

  test('ignores + includes', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {accounts: ['abc']},
      ignore: {tables: ['abc/fed']},
      shoulds: ['abc/table/def', 'abc/table/qed'],
    });
  });

  test('nonexistent table ignored', async function() {
    return backupTest({
      entities: defaultEntities,
      include: {},
      ignore: {accounts: ['foobarbaz']},
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
      ignore: {},
      shoulds: ['foo/table/bar'],
    });
  });
});
