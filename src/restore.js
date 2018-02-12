let _ = require('lodash');
let Promise = require('bluebird');
let zstd = require('node-zstd');
let symbols = require('./symbols');
let es = require('event-stream');

const restoreTables = async ({s3, azure, azureSAS, bucket, tables, concurrency}) => {
  await Promise.map(tables || [], async (tableConf, index) => {
    let symbol = symbols.choose(index);
    let {name: sourceName, remap} = tableConf;
    remap = remap || sourceName;
    console.log(`\nBeginning restore of table ${sourceName} to ${remap} with symbol ${symbol}`);

    let [accountId, tableName] = remap.split('/');

    let table = new azure.Table({
      accountId,
      sas: azureSAS.replace(/^\?/, ''),
    });

    await table.createTable(tableName).catch(async err => {
      if (err.code !== 'TableAlreadyExists') {
        throw err;
      }
      if ((await table.queryEntities(tableName, {top: 1})).entities.length > 0) {
        throw new Error(`Refusing to backup table ${sourceName} to ${remap}. ${remap} not empty!`);
      }
    });

    let [sAccountId, sTableName] = sourceName.split('/');
    let fetch = s3.getObject({
      Bucket: bucket,
      Key: `${sAccountId}/table/${sTableName}`,
    }).createReadStream().pipe(zstd.decompressStream()).pipe(es.split()).pipe(es.parse());

    await new Promise((accept, reject) => {
      let rows = 0;
      let promises = [];
      fetch.on('data', async row => {
        promises.push(table.insertEntity(tableName, row));
        if (rows++ % 1000 === 0) {
          symbols.write(symbol);
        }
      });
      fetch.on('end', _ => Promise.all(promises).then(accept).catch(reject));
      fetch.on('error', reject);
    });

    console.log(`\nFinished restore of table ${sourceName} to ${remap} with symbol ${symbol}`);
  }, {concurrency: concurrency});
};

const restoreContainers = async ({s3, azure, azureSAS, bucket, containers, concurrency}) => {
  await Promise.map(containers || [], async (containerConf, index) => {
    let symbol = symbols.choose(index);
    let {name: sourceName, remap} = containerConf;
    remap = remap || sourceName;
    console.log(`\nBeginning restore of container ${sourceName} to ${remap} with symbol ${symbol}`);

    let [accountId, containerName] = remap.split('/');

    let blobsvc = new azure.Blob({
      accountId,
      sas: azureSAS.replace(/^\?/, ''),
    });

    await blobsvc.createContainer(containerName).catch(async err => {
      if (err.code !== 'ContainerAlreadyExists') {
        throw err;
      }
      if ((await blobsvc.listBlobs(containerName, {maxResults: 1})).blobs.length > 0) {
        throw new Error(`Refusing to backup container ${sourceName} to ${remap}. ${remap} not empty!`);
      }
    });

    let [sAccountId, sContainerName] = sourceName.split('/');
    let fetch = s3.getObject({
      Bucket: bucket,
      Key: `${sAccountId}/container/${sContainerName}`,
    }).createReadStream().pipe(zstd.decompressStream()).pipe(es.split()).pipe(es.parse());

    const restoreBlob = async blob => {
      const res = await blobsvc.putBlob(
        containerName,
        blob.name, {
          metadata: blob.info.metadata,
          type: blob.info.type,
        }, blob.info.content);

      // https://github.com/taskcluster/fast-azure-storage/pull/28
      if ((res.contentMd5 || res.contentMD5) != blob.info.contentMD5) {
        throw new Error(`uploaded MD5 differed from that in backup of ${accountId}/${containerName} blob ${blob.name}`);
      }
    };

    await new Promise((accept, reject) => {
      let count = 0;
      let promises = [];
      fetch.on('data', async blob => {
        promises.push(restoreBlob(blob));
        if (count++ % 1000 === 0) {
          symbols.write(symbol);
        }
      });
      fetch.on('end', _ => Promise.all(promises).then(accept).catch(reject));
      fetch.on('error', reject);
    });

    console.log(`\nFinished restore of container ${sourceName} to ${remap} with symbol ${symbol}`);
  }, {concurrency: concurrency});
};

module.exports = {
  run: async ({s3, azure, azureSAS, bucket, tables, containers, concurrency}) => {
    console.log('Beginning restore.');

    symbols.setup();

    await Promise.all([
      restoreTables({s3, azure, azureSAS, bucket, tables, concurrency}),
      restoreContainers({s3, azure, azureSAS, bucket, containers, concurrency}),
    ]);

    console.log('\nFinished restore.');
  },
};
