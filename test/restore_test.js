suite('restore', () => {
  let assert = require('assert');
  let zstd = require('node-zstd');
  let _ = require('lodash');
  let mocks = require('./mocks');
  let Monitor = require('taskcluster-lib-monitor');
  let backup = require('../src/backup');
  let restore = require('../src/restore');
  let Promise = require('bluebird');
  let renderer = require('listr-silent-renderer');

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

  async function restoreTest({entities, resultAccounts, shoulds}) {
    azure.resetEntities();
    azure.resetContainers();
    azure.addAccounts(resultAccounts);
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
      ignore: {accounts: [], tables: []},
      include: {accounts: [], tables: []},
      concurrency: 10,
      monitor,
      renderer,
    });
    await restore.run({
      s3,
      azure,
      azureSAS: 'foo',
      bucket,
      tables: shoulds,
      concurrency: 10,
      renderer,
    });
    return Promise.each(shoulds, async should => {
      let [rAccountId, rTableName] = should.remap.split('/');
      let [nAccountId, nTableName] = should.name.split('/');
      let results = azure.getEntities()[rAccountId][rTableName];
      let orig = entities[nAccountId][nTableName];
      assert.equal(results.length, orig.length);
      results.forEach((res, index) => {
        assert.deepEqual(orig[index], res);
      });
    });
  }

  test('all', async function() {
    return restoreTest({
      entities: defaultEntities,
      resultAccounts: ['qqq', 'ddd'],
      shoulds: [
        {name: 'abc/def', remap: 'qqq/def'},
        {name: 'abc/fed', remap: 'qqq/fed'},
        {name: 'abc/qed', remap: 'qqq/qed'},
        {name: 'aaa/bbb', remap: 'ddd/bbb'},
      ],
    });
  });
});
