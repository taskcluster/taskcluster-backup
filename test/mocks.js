let _ = require('lodash');
let Promise = require('promise');
let streamifier = require('streamifier');

module.exports = {};

let entities = {};
let containers = {};

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
  }

  getObject({Bucket, Key}) {
    return {
      createReadStream: () => {
        return streamifier.createReadStream(this.things[Bucket + Key]);
      },
    };
  }
}

class mockAuth {
  azureTableSAS(account, tableName, level) {
    return {sas: 'foo123'};
  }
  async azureAccounts() {
    return {accounts: _.uniq(_.keys(entities).concat(_.keys(containers)))};
  }
  async azureTables(account) {
    return {tables: _.keys(entities[account])};
  }
  async azureContainers(account) {
    return {containers: _.keys(containers[account])};
  }
}

let mockAzure = {
  setEntities(account, tableName, rows) {
    entities[account] = entities[account] || {};
    entities[account][tableName] = rows || [];
  },
  resetEntities() {
    entities = {};
  },
  getEntities() {
    return entities;
  },
  setContainers(account, containerName, blobs) {
    containers[account] = containers[account] || {};
    containers[account][containerName] = blobs || [];
  },
  resetContainers() {
    containers = {};
  },
  getContainers() {
    return containers;
  },
  addAccounts(accounts) {
    _.forEach(accounts, account => {
      entities[account] = {};
    });
  },
  Table: class {

    constructor({accountId}) {
      this.account = accountId;
    }

    // We abuse tableParams.nextRowKey since it is opaque to
    // the consumer anyway and just use it as an index into
    // the array.
    async queryEntities(tableName, tableParams) {
      let queried = entities[this.account][tableName] || [];
      let top = tableParams.top || 10; // We set this to 10 for testing
      let rowKey = tableParams.nextRowKey || 0;
      let end = top + rowKey;
      let nextRowKey;
      if (end > queried.length) {
        end = queried.length;
      } else {
        nextRowKey = end;
      }
      let results = {
        entities: _.slice(queried, rowKey, end),
      };
      if (nextRowKey) {
        results.nextPartitionKey = 'whatever';
        results.nextRowKey = nextRowKey;
      }
      return results;
    }

    async createTable(tableName) {
      entities[this.account][tableName] = [];
    }

    async insertEntity(tableName, entity) {
      entities[this.account][tableName].push(entity);
    }
  },
  Blob: class {

    constructor({accountId}) {
      this.account = accountId;
    }

    async listBlobs(containerName, options) {
      // just return one at a time..
      let blobs = containers[this.account][containerName] || [];
      let index = options.marker || 0;
      return {
        blobs: [{name: blobs[index].name}],
        nextMarker: index + 1 < blobs.length ? index + 1 : undefined,
      };
    }

    async getBlob(containerName, blobName, options) {
      let blobs = containers[this.account][containerName] || [];
      let blob = _.find(blobs, {name: blobName});
      return {
        content: blob.content,
      };
    }
  },
};

module.exports = {s3: mockS3, auth: mockAuth, azure: mockAzure};
