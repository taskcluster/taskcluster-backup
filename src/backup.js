let _ = require('lodash');
let Promise = require('bluebird');
let zstd = require('node-zstd');
let Listr = require('listr');
let debug = require('debug')('backup');

let getAccounts = async (auth, included, ignored) => {
  let accounts = included;
  if (accounts.length === 0) {
    debug('No accounts in config. Loading accounts from auth service...');
    accounts = (await auth.azureAccounts()).accounts;
  }
  debug(`Full list of available accounts: ${JSON.stringify(accounts)}`);
  let extraIgnored = _.difference(ignored, accounts);
  if (extraIgnored.length !== 0) {
    throw new Error(`
      Ignored acccounts ${JSON.stringify(extraIgnored)} are not in set ${JSON.stringify(accounts)}. Aborting.
    `);
  }
  return _.difference(accounts, ignored);
};

let getTables = async (auth, account, included, ignored) => {
  let tables = included.filter(table => table.startsWith(account + '/')).map(table => table.split('/')[1]);
  if (tables.length === 0) {
    debug(`No tables in config for account ${account}. Loading tables from auth service...`);
    let accountParams = {};
    do {
      let resp = await auth.azureTables(account, accountParams);
      accountParams.continuationToken = resp.continuationToken;
      tables = tables.concat(resp.tables);
    } while (accountParams.continuationToken);
  }

  let ignoreTables = ignored.filter(table => table.startsWith(account + '/')).map(table => table.split('/')[1]);
  let extraIgnored = _.difference(ignoreTables, tables);
  if (extraIgnored.length !== 0) {
    throw new Error(`
      Ignored tables ${JSON.stringify(extraIgnored)} are not tables in ${JSON.stringify(tables)}. Aborting.
    `);
  }
  return _.difference(tables, ignoreTables);
};

let backupTables = async ({cfg, auth, s3, azure, bucket, accounts, include, ignore, concurrency, monitor}) => {
  debug('Beginning table backup.');
  debug('Including tables: ' + JSON.stringify(include.tables));
  debug('Ignoring tables: ' + JSON.stringify(ignore.tables));

  let tables = [];
  await Promise.each(accounts, async account => {
    let ts = await getTables(auth, account, include.tables || [], ignore.tables || []);
    tables = tables.concat(ts.map(t => [account, t]));
  });
  debug('Backing up tables: ' + JSON.stringify(tables));

  debug(`Backing up ${tables.length} tables`);
  monitor.count('tables', tables.length);

  return Promise.map(tables, ([account, tableName], index) => ({
    title: `Table ${account}/${tableName}`,
    task: async () => {
      let count = 0;
      return monitor.timer(`backup-${account}.${tableName}`, async () => {
        let stream = new zstd.compressStream();

        let table = new azure.Table({
          accountId: account,
          sas: async _ => {
            return (await auth.azureTableSAS(account, tableName, 'read-only')).sas;
          },
        });

        // Versioning is enabled in the backups bucket so we just overwrite the
        // previous backup every time. The bucket is configured to delete previous
        // versions after N days, but the current version will never be deleted.
        let upload = s3.upload({
          Bucket: bucket,
          Key: `${account}/table/${tableName}`,
          Body: stream,
          StorageClass: 'STANDARD_IA',
        }).promise();

        let processEntities = entities => entities.map(entity => {
          stream.write(JSON.stringify(entity) + '\n');
        });

        let tableParams = {};
        do {
          let results = await table.queryEntities(tableName, tableParams);
          tableParams = _.pick(results, ['nextPartitionKey', 'nextRowKey']);
          processEntities(results.entities);
          count += results.entities.length;
        } while (tableParams.nextPartitionKey && tableParams.nextRowKey);

        stream.end();
        await upload;
      });
    },
  }));
};

let getContainers = async (auth, account, included, ignored) => {
  let containers = included.filter(
    container => container.startsWith(account + '/')).map(container => container.split('/')[1]);
  if (containers.length === 0) {
    debug(`No containers in config for account ${account}. Loading containers from auth service...`);
    let accountParams = {};
    do {
      let resp = await auth.azureContainers(account, accountParams);
      accountParams.continuationToken = resp.continuationToken;
      containers = containers.concat(resp.containers);
    } while (accountParams.continuationToken);
  }

  let ignoreContainers = ignored.filter(
    container => container.startsWith(account + '/')).map(container => container.split('/')[1]);
  let extraIgnored = _.difference(ignoreContainers, containers);
  if (extraIgnored.length !== 0) {
    throw new Error(`
      Ignored containers ${JSON.stringify(extraIgnored)} are not containers in ${JSON.stringify(containers)}. Aborting.
    `);
  }
  return _.difference(containers, ignoreContainers);
};

let backupContainers = async ({cfg, auth, s3, azure, bucket, accounts, include, ignore, concurrency, monitor}) => {
  debug('Beginning container backup.');
  debug('Including containers: ' + JSON.stringify(include.containers));
  debug('Ignoring containers: ' + JSON.stringify(ignore.containers));

  let containers = [];
  await Promise.each(accounts, async account => {
    let ts = await getContainers(auth, account, include.containers || [], ignore.containers || []);
    containers = containers.concat(ts.map(t => [account, t]));
  });
  debug('Backing up containers: ' + JSON.stringify(containers));

  debug(`Backing up ${containers.length} containers`);
  monitor.count('containers', containers.length);

  return Promise.map(containers, ([account, containerName], index) => ({
    title: `Container ${account}/${containerName}`,
    task: async () => {
      return monitor.timer(`backup-${account}.${containerName}`, async () => {
        let count = 0;

        let stream = new zstd.compressStream();

        let container = new azure.Blob({
          accountId: account,
          sas: async _ => {
            return (await auth.azureContainerSAS(account, containerName, 'read-only')).sas;
          },
        });

        // Versioning is enabled in the backups bucket so we just overwrite the
        // previous backup every time. The bucket is configured to delete previous
        // versions after N days, but the current version will never be deleted.
        let upload = s3.upload({
          Bucket: bucket,
          Key: `${account}/container/${containerName}`,
          Body: stream,
          StorageClass: 'STANDARD_IA',
        }).promise();

        let marker;
        do {
          let results = await container.listBlobs(containerName, {marker});
          for (let blob of results.blobs) {
            // NOTE: this only gets *some* of the data associated with the blob; notably,
            // it omits properties
            //   (https://taskcluster.github.io/fast-azure-storage/classes/Blob.html#method-getBlobMetadata)
            // These are not currently used by the azure-blob-storage library
            let blobInfo = await container.getBlob(containerName, blob.name, {});
            stream.write(JSON.stringify({name: blob.name, info: blobInfo}) + '\n');
          }
          count += results.blobs.length;
          marker = results.nextMarker;
        } while (marker);

        stream.end();
        await upload;
      });
    },
  }));
};

module.exports = {
  run: async ({cfg, auth, s3, azure, bucket, include, ignore, concurrency, monitor, renderer}) => {
    const backup = new Listr([{
      title: 'Determine Objects to Backup',
      task: async ctx => {
        monitor.count('begin');

        debug('Ignoring accounts: ' + JSON.stringify(ignore.accounts || []));
        debug('Including accounts: ' + JSON.stringify(include.accounts || []));
        let accounts = await getAccounts(auth, include.accounts || [], ignore.accounts || []);
        debug('Backing up accounts: ' + JSON.stringify(accounts));

        ctx.backupTasks = []
          .concat(await backupTables({cfg, auth, s3, azure, bucket, accounts, include, ignore, monitor}))
          .concat(await backupContainers({cfg, auth, s3, azure, bucket, accounts, include, ignore, monitor}));
      },
    }, {
      title: 'Backup',
      task: ctx => new Listr(ctx.backupTasks, {concurrency}),
    }, {
      title: 'Cleanup',
      task: async () => {
        monitor.count('end');
        monitor.stopResourceMonitoring();
        await monitor.flush();
      },
    }], {renderer});

    await monitor.timer('total-backup', () => backup.run());
  },
};
