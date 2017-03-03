let _ = require('lodash');
let chalk = require('chalk');

let symbols;

module.exports = {
  setup: () => {
    let colors = ['red', 'blue', 'green', 'yellow'];
    let glyphs = '⚀∅⚁®⚂©⚃℗⚄❀☀☁☂♩❖♫★☆☉☘☢✪♔♕♖♗♘⚑';
    symbols = _.flatMap(glyphs, s => colors.map(c => [c, s]));
  },
  choose: (index) => {
    if (!symbols) {
      console.log('Must set up symbols before use!');
      process.exit(1);
    }
    let si = symbols[index % symbols.length];
    return chalk[si[0]](si[1]);
  },
};
