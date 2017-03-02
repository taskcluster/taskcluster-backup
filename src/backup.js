let _ = require('lodash');
let Promise = require('bluebird');
let AWS = require('aws-sdk');
let zstd = require('node-zstd');
let azure = require('fast-azure-storage');
let chalk = require('chalk');

let getAccounts = async (auth, ignored) => {
  let accounts = (await auth.azureAccounts()).accounts;
  console.log(`Full list of available accounts: ${JSON.stringify(accounts)}`);
  let extraIgnored = _.difference(ignored, accounts);
  if (extraIgnored.length !== 0) {
    console.log(`Ignored acccounts ${JSON.stringify(extraIgnored)} are not accounts. Aborting.`);
    process.exit(1);
  }
  return _.difference(accounts, ignored);
};

let filterTables = (account, tables, ignored) => {
  let ignoreTables = ignored.filter(table => table.startsWith(account + '/')).map(table => table.split('/')[1]);
  let extraIgnored = _.difference(ignoreTables, tables);
  if (extraIgnored.length !== 0) {
    console.log(`Ignored tables ${JSON.stringify(extraIgnored)} are not tables in ${account}. Aborting.`);
    process.exit(1);
  }
  return _.difference(tables, ignoreTables);
};

let setupSymbols = () => {
  let colors = ['red', 'blue', 'green', 'yellow'];
  let glyphs = '⚀∅⚁®⚂©⚃℗⚄❀☀☁☂♩❖♫★☆☉☘☢✪♔♕♖♗♘⚑';
  return _.flatMap(glyphs, s => colors.map(c => [c, s]));
};

let chooseSymbol = (index, symbols) => {
  let si = symbols[index % symbols.length];
  return chalk[si[0]](si[1]);
};

module.exports = {
  run: async ({cfg, auth}) => {
    console.log('Beginning backup.');
    console.log('Ignoring accounts: ' + JSON.stringify(cfg.ignore.accounts));
    console.log('Ignoring tables: ' + JSON.stringify(cfg.ignore.tables));

    let s3 = new AWS.S3({
      credentials: (await auth.awsS3Credentials('read-write', cfg.s3.bucket, '')).credentials,
    });

    let accounts = await getAccounts(auth, cfg.ignore.accounts);
    let symbols = setupSymbols();

    await Promise.each(accounts, async account => {
      console.log('\nBeginning backup of ' + account);

      let accountParams = {};
      do {
        let resp = await auth.azureTables(account, accountParams);
        accountParams.continuationToken = resp.continuationToken;
        let tables = filterTables(account, resp.tables, cfg.ignore.tables);

        await Promise.map(tables, async (tableName, index) => {
          let symbol = chooseSymbol(index, symbols);
          console.log(`\nBeginning backup of ${account}/${tableName} with symbol ${symbol}`);

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
            Bucket: cfg.s3.bucket,
            Key: `${account}/${tableName}`,
            Body: stream,
            StorageClass: 'STANDARD_IA',
          }).promise();

          let processEntities = entities => entities.map(entity => {
            stream.write(JSON.stringify(entity));
          });

          let tableParams = {};
          do {
            let results = await table.queryEntities(tableName, tableParams);
            tableParams = _.pick(results, ['nextPartitionKey', 'nextRowKey']);
            process.stdout.write(symbol);
          } while (tableParams.nextPartitionKey && tableParams.nextRowKey);

          stream.end();
          await upload;
          console.log(`\nFinishing backup of ${account}/${tableName} (${symbol})`);
        }, {concurrency: cfg.concurrency});
      } while (accountParams.continuationToken);

      console.log(`\nFinishing backup of ${account}`);
    });
    console.log('\nFinished backup.');
  },
};
