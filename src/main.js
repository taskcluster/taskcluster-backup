let AWS = require('aws-sdk');
let config = require('typed-env-config');
let loader = require('taskcluster-lib-loader');
let taskcluster = require('taskcluster-client');
let azure = require('fast-azure-storage');
let backup = require('./backup');
let restore = require('./restore');

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
          baseUrl: 'taskcluster/auth/v1/',
        });
      }
      return auth;
    },
  },

  s3: {
    requires: ['cfg', 'auth'],
    setup: async ({cfg, auth}) => {
      let credentials;
      if (cfg.restore.s3.accessKeyId && cfg.restore.s3.secretAccessKey) {
        credentials = cfg.restore.s3;
      } else {
        credentials = (await auth.awsS3Credentials('read-write', cfg.s3.bucket, '')).credentials;
      }
      return new AWS.S3({
        credentials,
      });
    },
  },

  backup: {
    requires: ['cfg', 'auth', 's3'],
    setup: async ({cfg, auth, s3}) => {
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
      });
    },
  },

  restore: {
    requires: ['cfg'],
    setup: async ({cfg}) => {
      cfg.include.accounts = cfg.include.accounts || [];
      cfg.include.tables = cfg.include.tables || [];
      cfg.ignore.accounts = cfg.ignore.accounts || [];
      cfg.ignore.tables = cfg.ignore.tables || [];
      return await restore.run({
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
