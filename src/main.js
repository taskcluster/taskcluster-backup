let config = require('typed-env-config');
let loader = require('taskcluster-lib-loader');
let taskcluster = require('taskcluster-client');
let backup = require('./backup');

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
          baseUrl: 'taskcluster/auth/v1/'
        });
      }
      return auth;
    },
  },

  backup: {
    requires: ['cfg', 'auth'],
    setup: async ({cfg, auth}) => {
      cfg.include.accounts = cfg.include.accounts || [];
      cfg.include.tables = cfg.include.tables || [];
      cfg.ignore.accounts = cfg.ignore.accounts || [];
      cfg.ignore.tables = cfg.ignore.tables || [];
      return await backup.run({cfg, auth, ignore: cfg.ignore, include: cfg.include});
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
