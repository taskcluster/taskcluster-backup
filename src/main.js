let _ = require('lodash');
let Promise = require('bluebird');
let AWS = require('aws-sdk');
let zstd = require('node-zstd');
let azure = require('fast-azure-storage');
let config = require('typed-env-config');
let loader = require('taskcluster-lib-loader');
let taskcluster = require('taskcluster-client');
let chalk = require('chalk');

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

      let accounts = _.difference((await auth.azureAccounts()).accounts, cfg.ignore.accounts);


      let colors = ['red', 'blue', 'green', 'yellow'];
      let glyphs = '☀☁☂★☆☉☘☢♔♕♖♗♘⚑';
      let symbols = _.shuffle(_.flatMap(glyphs, s => colors.map(c => [c, s])));

      await Promise.each(accounts, async account => {
        console.log('\nBeginning backup of: ' + account);

        let accountParams = {};
        do {
          let resp = await auth.azureTables(account, accountParams);
          accountParams.continuationToken = resp.continuationToken;
          let tables = _.difference(resp.tables, cfg.ignore.tables)
          await Promise.map(tables, async (tableName, index) => {
            let si = symbols[index % symbols.length];
            let symbol = chalk.bold[si[0]](si[1]);
            console.log(`\nBeginning backup of: ${account}/${tableName} with symbol ${symbol}`);

            let stream = new zstd.compressStream();
            let table = new azure.Table({
              accountId: account,
              sas: async _ => {
                return (await auth.azureTableSAS(account, tableName, 'read-only')).sas;
              },
            });

            let upload =  s3.upload({
              Bucket: cfg.s3.bucket,
              Key: `${account}/${tableName}/${taskcluster.fromNow().toJSON()}`,
              Body: stream,
              Expires: expires,
            }).promise();

            let processEntities = entities => entities.map(entity => {
              stream.write(JSON.stringify(entity));
            });

            let tableParams = {};
            do {
              let results = await table.queryEntities(tableName, tableParams);
              tableParams = _.pick(results, ['nextPartitionKey', 'nextRowKey']);
              process.stdout.write(chalk['green'](symbol));
            } while (tableParams.nextPartitionKey && tableParams.nextRowKey);

            stream.end();
            await upload;
            console.log('\nFinishing backup of: ' + account + '/' + tableName);
          }, {concurrency: cfg.concurrency});
        } while (accountParams.continuationToken);

        console.log('\nFinishing backup of: ' + account);
      });
      console.log('\nFinished backup.');
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
