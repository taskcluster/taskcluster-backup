const assert = require('assert');
const fs = require('fs');
const zstd = require('node-zstd');
const es = require('event-stream');
const pumpify = require('pumpify');

class File {
  constructor(url) {
    assert(url.protocol === 'file:');
    assert(!url.hostname, 'file URLs cannot have hostnames (use file:///)');
    assert(url.pathname.endsWith('.zst'), 'pathname must end with .zst');

    this.filename = url.pathname;
  }

  /**
   * Returns a readable stream containing data from this file
   */
  read() {
    return pumpify.obj(
      fs.createReadStream(this.filename),
      zstd.decompressStream(),
      es.split(),
      es.parse(),
    );
  }

  /**
   * Returns a writeable stream that will write the data to this file
   */
  write() {
    return pumpify.obj(
      es.stringify(),
      new zstd.compressStream(),
      fs.createWriteStream(this.filename),
    );
  }
}

exports.File = File;
