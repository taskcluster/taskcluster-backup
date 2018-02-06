let AWS = require('aws-sdk');
let config = require('typed-env-config');
let loader = require('taskcluster-lib-loader');
let taskcluster = require('taskcluster-client');
let monitor = require('taskcluster-lib-monitor');
let azure = require('fast-azure-storage');
let backup = require('./backup');
let restore = require('./restore');
let _ = require('lodash');
let jsonpatch = require('fast-json-patch');
let hash = require('object-hash');

let load = loader({
  cfg: {
    requires: ['profile'],
    setup: ({profile}) => config({profile}),
  },

  monitor: {
    requires: ['process', 'profile', 'cfg'],
    setup: ({process, profile, cfg}) => monitor({
      project: 'taskcluster-backups',
      bailOnUnhandledRejection: true,
      credentials: cfg.taskcluster.credentials,
      authBaseUrl: 'taskcluster/auth/v1/',
      mock: profile === 'test',
      process,
    }),
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
          baseUrl: 'taskcluster/auth/v1/',
        });
      }
      return auth;
    },
  },

  s3: {
    requires: ['cfg', 'auth', 'monitor'],
    setup: async ({cfg, auth, monitor}) => {
      let credentials;
      if (cfg.restore.aws.accessKeyId && cfg.restore.aws.secretAccessKey) {
        credentials = cfg.restore.aws;
      } else {
        // We make this creds class to allow refreshing creds in the middle of
        // uploading a large backup
        class Creds extends AWS.Credentials {
          refresh(cb) {
            if (!cb) {
              cb = err => {if (err) {throw err;}};
            }
            auth.awsS3Credentials('read-write', cfg.s3.bucket, '', {format: 'iam-role-compat'}).then(values => {
              this.expired = false;
              this.accessKeyId = values.AccessKeyId;
              this.secretAccessKey = values.SecretAccessKey;
              this.sessionToken = values.Token;
              this.expireTime = new Date(values.Expiration);
              cb();
            }).catch(cb);
          }
        }
        credentials = new Creds();
      }
      let s3 = new AWS.S3({credentials});
      monitor.patchAWS(s3);
      return s3;
    },
  },

  backup: {
    requires: ['cfg', 'auth', 's3', 'monitor'],
    setup: async ({cfg, auth, s3, monitor}) => {
      return await backup.run({
        cfg,
        auth,
        s3,
        azure,
        bucket: cfg.s3.bucket,
        ignore: cfg.ignore,
        include: cfg.include,
        concurrency: cfg.concurrency,
        monitor,
      });
    },
  },

  restore: {
    requires: ['cfg', 's3'],
    setup: async ({cfg, s3}) => {
      return await restore.run({
        s3,
        azure,
        azureSAS: cfg.restore.azure.sas,
        bucket: cfg.s3.bucket,
        tables: cfg.restore.tables,
        concurrency: cfg.concurrency,
      });
    },
  },

  verify: {
    requires: ['cfg', 'auth'],
    setup: async ({cfg, auth}) => {
      let fetchRows = async (verifyTable) => {
        let entities = {};
        let [account, tableName] = verifyTable.trim().split('/');
        let table = new azure.Table({
          accountId: account,
          sas: async _ => {
            return (await auth.azureTableSAS(account, tableName, 'read-only')).sas;
          },
        });
        let tableParams = {};
        let removeFields = [
          'odata.type', // This is the name of the table and so is obviously different
          'odata.id', // This changes based on the name of the table
          'odata.etag', // This changes based on azure-specific timestamps
          'odata.editLink', // This is table specific as well
          'Timestamp', // This is updated by azure itself, but is not used by taskcluster
        ];
        do {
          let results = await table.queryEntities(tableName, tableParams);
          tableParams = _.pick(results, ['nextPartitionKey', 'nextRowKey']);
          results.entities.forEach(ent => {
            ent = _.omit(ent, removeFields);
            entities[hash(ent)] = ent;
          });
        } while (tableParams.nextPartitionKey && tableParams.nextRowKey);
        return entities;
      };

      let diffs = [];
      let table1 = await fetchRows(cfg.verify.table1, table1);
      let table2 = await fetchRows(cfg.verify.table2, table2);

      for (let t of _.intersection(_.keys(table1), _.keys(table2))) {
        let diff = jsonpatch.compare(table1[t], table2[t]);
        if (diff.length) {
          diffs.push(diff);
        }
      }

      if (cfg.verify.diffs) {
        console.log(diffs);
      } else {
        console.log('To see diffs between objects, run again after setting VERIFY_DIFFS=true');
      }
      console.log(`Number of entities in table1: ${_.keys(table1).length}`);
      console.log(`Number of entities in table2: ${_.keys(table2).length}`);
      console.log(`Number of entities in table2 not in table1: ${_.keys(table2).length - _.keys(table1).length}`);
      console.log(`Number of entities that are different between tables: ${diffs.length}`);
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
