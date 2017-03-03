let AWS = require('aws-sdk');
let config = require('typed-env-config');
let loader = require('taskcluster-lib-loader');
let taskcluster = require('taskcluster-client');
let monitor = require('taskcluster-lib-monitor');
let azure = require('fast-azure-storage');
let backup = require('./backup');
let restore = require('./restore');

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
      if (cfg.restore.s3.accessKeyId && cfg.restore.s3.secretAccessKey) {
        credentials = cfg.restore.s3;
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
      cfg.include.accounts = cfg.include.accounts || [];
      cfg.include.tables = cfg.include.tables || [];
      cfg.ignore.accounts = cfg.ignore.accounts || [];
      cfg.ignore.tables = cfg.ignore.tables || [];
      return await backup.run({
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
      cfg.include.accounts = cfg.include.accounts || [];
      cfg.include.tables = cfg.include.tables || [];
      cfg.ignore.accounts = cfg.ignore.accounts || [];
      cfg.ignore.tables = cfg.ignore.tables || [];
      return await restore.run({
        s3,
        bucket: cfg.s3.bucket,
        ignore: cfg.ignore,
        include: cfg.include,
        concurrency: cfg.concurrency,
      });
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
