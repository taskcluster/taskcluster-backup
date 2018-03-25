const {URL} = require('url');
const {File} = require('./file');

/**
 * Create a new endpoint based on a URL.
 *
 * An endpoint is an object with the following methods:
 *
 * - write() -- returns a writable stream that will write object
 *              data to this endpoint
 * - reqad() -- returns a readable stream that will read object data
 *              from this endpoint
 */
const endpoint = (url, cfg) => {
  if (typeof url === 'string') {
    url = new URL(url);
  }

  switch (url.protocol) {
    case 'file:':
      return new File(url);
    default:
      throw new Error(`invalid data URL ${url}`);
  }
};

exports.endpoint = endpoint;
