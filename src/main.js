const program = require('commander');
const {version} = require('../package.json');

const profile = process.env.NODE_ENV || 'development';

program.version(version);
program.command('backup')
  .action(() => {
    require('./loader')('backup', {
      process: 'backup',
      profile,
    }).catch(err => {
      console.log(err.stack);
      process.exit(1);
    });
  });

program.command('restore')
  .action(() => {
    require('./loader')('restore', {
      process: 'restore',
      profile,
    }).catch(err => {
      console.log(err.stack);
      process.exit(1);
    });
  });

program.command('verify')
  .action(() => {
    require('./loader')('verify', {
      process: 'verify',
      profile,
    }).catch(err => {
      console.log(err.stack);
      process.exit(1);
    });
  });

program.command('cp <source> <destination>')
  .action((source, destination) => {
    require('./loader')('cp', {
      process: 'cp',
      profile,
      cmdline: {
        source,
        destination,
      },
    }).catch(err => {
      console.log(err.stack);
      process.exit(1);
    });
  });

program.command('*', {noHelp: true})
  .action(() => program.help(txt => txt));

program.parse(process.argv);
if (!program.args.length) {
  program.help();
}
