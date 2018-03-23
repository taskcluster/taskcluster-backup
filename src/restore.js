let _ = require('lodash');
let Promise = require('bluebird');
let zstd = require('node-zstd');
let es = require('event-stream');
let Listr = require('listr');
let debug = require('debug')('restore');

const restoreTables = async ({s3, azure, azureSAS, bucket, tables, concurrency}) => {
  return (tables || []).map((tableConf, index) => ({
    title: `Table ${tableConf.name} -> ${tableConf.remap}`,
    task: async () => {
      let {name: sourceName, remap} = tableConf;
      remap = remap || sourceName;
      debug(`Beginning restore of table ${sourceName} to ${remap}`);

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
          throw new Error(`Refusing to restore table ${sourceName} to ${remap}. ${remap} not empty!`);
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
        });
        fetch.on('end', _ => Promise.all(promises).then(accept).catch(reject));
        fetch.on('error', reject);
      });

      debug(`Finished restore of table ${sourceName} to ${remap}`);
    },
  }));
};

const restoreContainers = async ({s3, azure, azureSAS, bucket, containers, concurrency}) => {
  return (containers || []).map((containerConf, index) => ({
    title: `Container ${containerConf.name} -> ${containerConf.remap}`,
    task: async () => {
      let {name: sourceName, remap} = containerConf;
      remap = remap || sourceName;
      debug(`Beginning restore of container ${sourceName} to ${remap}`);

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
        });
        fetch.on('end', _ => Promise.all(promises).then(accept).catch(reject));
        fetch.on('error', reject);
      });

      debug(`Finished restore of container ${sourceName} to ${remap}`);
    },
  }));
};

module.exports = {
  run: async ({s3, azure, azureSAS, bucket, tables, containers, concurrency, renderer}) => {
    const restore = new Listr([{
      title: 'Restore',
      task: async () => {
        const restoreTasks = []
          .concat(await restoreTables({s3, azure, azureSAS, bucket, tables, concurrency}))
          .concat(await restoreContainers({s3, azure, azureSAS, bucket, containers, concurrency}));
        return new Listr(restoreTasks, {concurrency});
      },
    }], {renderer});

    await restore.run();
  },
};
