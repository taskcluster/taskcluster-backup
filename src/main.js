let AWS = require('aws-sdk');
let zstd = require('node-zstd');
let azure = require('fast-azure-storage');
let config = require('typed-env-config');
let loader = require('taskcluster-lib-loader');
let taskcluster = require('taskcluster-client');

let load = loader({
  cfg: {
    requires: ['profile'],
    setup: ({profile}) => config({profile}),
  },

  auth: {
    requires: ['cfg'],
    setup: async ({cfg}) => {
      let auth;
      if (cfg.taskcluster.credentials.clientId) {
        auth = new taskcluster.Auth({
          credentials: cfg.taskcluster.credentials,
        });
      } else {
        auth = new taskcluster.Auth({
          baseUrl: 'taskcluster/auth/'
        });
      }
      return auth;
    },
  },

  backup: {
    requires: ['cfg', 'auth'],
    setup: async ({cfg, auth}) => {
      let expires = taskcluster.fromNow(cfg.s3.expires);
      console.log('Beginning backup. All results set to expire at ' + expires);
      let s3 = new AWS.S3({
        credentials: (await auth.awsS3Credentials('read-write', cfg.s3.bucket, '')).credentials,
      });

      cfg.tables.map(async pair => {
        console.log('Beginning backing up of ' + pair);
        let stream = new zstd.compressStream();

        let [accountId, tableName] = pair.split('/');
        let table = new azure.Table({
          accountId,
          sas: async _ => {
            return (await auth.azureTableSAS(accountId, tableName)).sas;
          },
        });

        let upload =  s3.upload({
          Bucket: cfg.s3.bucket,
          Key: `${accountId}/${tableName}/${taskcluster.fromNow().toJSON()}`,
          Body: stream,
          Expires: expires,
        }).promise();

        let processEntities = entities => entities.map(entity => {
          stream.write(JSON.stringify(entity));
        });

        let results = await table.queryEntities(tableName);
        processEntities(results.entities);
        while (true) {
          results = await table.queryEntities(tableName, {
            nextPartitionKey: results.nextPartitionKey,
            nextRowKey: results.nextRowKey
          });
          processEntities(results.entities);
          if (!results.nextPartitionKey || !results.nextRowKey) {
            stream.end();
            break;
          }
        }

        await upload;
        console.log('Finished backing up ' + pair);
      });
      console.log('Backup Complete!');
    },
  },
}, ['profile', 'process']);

if (!module.parent) {
  load(process.argv[2], {
    process: process.argv[2],
    profile: process.env.NODE_ENV,
  }).catch(err => {
    console.log(err.stack);
    process.exit(1);
  });
}

// Export load for tests
module.exports = load;
