let _ = require('lodash');
let Promise = require('promise');

module.exports = {};

class mockS3 {
  constructor() {
    this.things = {};
  }

  upload({Bucket, Key, Body, StorageClass}) {

    this.things[Bucket + Key] = Buffer.alloc(0);
    Body.on('data', chunk => {
      this.things[Bucket + Key] = Buffer.concat([this.things[Bucket + Key], chunk]);
    });

    let finished = new Promise((resolve, reject) => {
      Body.on('end', _ => {
        resolve();
      });
    });
    return {promise: _ => finished};
  };
}

class mockAuth {
  azureTableSAS(account, tableName, level) {
    return {sas: 'foo123'};
  }
}

let entities = [];
let mockAzure = {
  setEntities(rows) {
    entities = rows || [];
  },
  Table: class {
    // We abuse tableParams.nextRowKey since it is opaque to
    // the consumer anyway and just use it as an index into
    // the array.
    queryEntities(tableName, tableParams) {
      let top = tableParams.top || 1000;
      let rowKey = tableParams.nextRowKey || 0;
      let end = top + rowKey;
      let nextRowKey;
      if (end > entities.length) {
        end = entities.length;
      } else {
        nextRowKey = end;
      }
      let results = {
        entities: _.slice(entities, rowKey, end + 1),
      };
      if (nextRowKey) {
        results.nextPartitionKey = 'whatever';
        results.nextRowKey = nextRowKey;
      }
      return results;
    }
  },
};

module.exports = {s3: mockS3, auth: mockAuth, azure: mockAzure};
