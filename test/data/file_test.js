const assert = require('assert');
const {File} = require('../../src/data/file');
const {URL} = require('url');
const fs = require('fs');
const path = require('path');
const zstd = require('node-zstd');
const {Readable} = require('stream');
const es = require('event-stream');
const pump = require('pump');
const debugStream = require('debug-stream')('stream');
const {CollectObjects} = require('stream-collect');

CONTENT = '{"nr":1}\n{"nr":2}\n{"nr":3}\n';
COMPRESSED_CONTENT = zstd.compressSync(new Buffer(CONTENT));

suite('File', function() {
  const filename1 = 'test-data1.zst';
  const url1 = new URL('file:///' + path.resolve(filename1));
  const filename2 = 'test-data2.zst';
  const url2 = new URL('file:///' + path.resolve(filename2));

  teardown(function() {
    [filename1, filename2].forEach(fn => {
      if (fs.existsSync(fn)) {
        fs.unlinkSync(fn);
      }
    });
  });

  const sourceStream = () => {
    return new Readable({objectMode: true}).wrap(es.readArray([{nr: 1}, {nr: 2}, {nr: 3}]));
  };

  test('read file', async function() {
    fs.writeFileSync(filename1, COMPRESSED_CONTENT);
    const file = new File(url1);

    const got = await pump(
      file.read(),
      debugStream('pipeline'),
      new CollectObjects());
    assert.deepEqual(got, [{nr: 1}, {nr: 2}, {nr: 3}]);
  });

  test('write file', async function() {
    const file = new File(url1);

    await new Promise((resolve, reject) => {
      pump(
        sourceStream(),
        debugStream('pipeline'),
        file.write(),
        err => err ? reject(err) : resolve());
    });

    const result = zstd.decompressSync(fs.readFileSync(filename1));
    assert.equal(result.toString(), CONTENT);
  });

  test('read | write', async function() {
    fs.writeFileSync(filename1, COMPRESSED_CONTENT);
    const file1 = new File(url1);
    const file2 = new File(url2);

    await new Promise((resolve, reject) => {
      pump(
        file1.read(),
        debugStream('pipeline'),
        file2.write(),
        err => err ? reject(err) : resolve());
    });

    const result = zstd.decompressSync(fs.readFileSync(filename2));
    assert.equal(result.toString(), CONTENT);
  });
});
