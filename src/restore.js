let _ = require('lodash');
let Promise = require('bluebird');
let zstd = require('node-zstd');
let symbols = require('./symbols');

let getTables = async (s3, bucket, included, ignored) => {
  let tables = _.map((await s3.listObjects({Bucket: bucket, Prefix: ''}).promise()).Contents, 'Key');
  console.log(`Full list of available tables: ${JSON.stringify(tables)}`);

  included.accounts.forEach(account => {
    tables = tables.filter(table => table.startsWith(account + '/'));
  });
  return tables;
};

module.exports = {
  run: async ({s3, azure, bucket, include, ignore, concurrency}) => {
    console.log('Beginning restore.');
    console.log('Ignoring accounts: ' + JSON.stringify(ignore.accounts));
    console.log('Ignoring tables: ' + JSON.stringify(ignore.tables));

    symbols.setup();

    let tables = await getTables(s3, bucket, include, ignore);

    console.log('NOT YET IMPLEMENTED!');
    process.exit(1);

    await Promise.map(tables, async (tableName, index) => {
      let symbol = symbols.choose(index);
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
        Bucket: bucket,
        Key: `${account}/${tableName}`,
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
        process.stdout.write(symbol);
      } while (tableParams.nextPartitionKey && tableParams.nextRowKey);

      stream.end();
      await upload;
      console.log(`\nFinishing backup of ${account}/${tableName} (${symbol})`);
    }, {concurrency: concurrency});

    console.log('\nFinished restore.');
  },
};
