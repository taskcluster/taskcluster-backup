const assert = require('assert');
const endpoint = require('../../src/data');
const {URL} = require('url');

suite('endpoint', function() {
  test('invalid protocol throws', function() {
    assert.throws(() => endpoint('foobar:///xyz'), /invalid data URL/);
  });

  test('string as argument', function() {
    const ep = endpoint('file:///abc.zstd');
    assert(ep.constructor.name === 'File');
  });

  test('URL as argument', function() {
    const ep = endpoint(new URL('file:///abc.zstd'));
    assert(ep.constructor.name === 'File');
  });
});
