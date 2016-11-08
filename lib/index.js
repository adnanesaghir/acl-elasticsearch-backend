'use strict';

/**
 ElasticSearch Backend.
 Implementation of the storage backend using ElasticSearch
 */

var contract = require('./contract');
var async = require('async');
var _ = require('lodash');

/**
 * Main function
 * @param params
 * @param params.client - ES Client
 * @param params.index - ES index, default to 'acl'
 * @param params.prefix - prefix for ES types, default to none
 * @param params.refresh - ES refresh option, see ES docs, default to false
 * @param params.size - ES search size limit, to bypass default value (10)
 * @constructor
 */
function ElasticSearchBackend(params) {
  if (!(params && typeof params == 'object')) {
    throw new Error('Invalid parameters');
  }
  if (!params.client) {
    throw new Error('Missing ES client parameter');
  }
  this.client = params.client;
  this.index = 'index' in params ? params.index : 'acl'; // Elasticsearch Index
  this.prefix = 'prefix' in params ? params.prefix : ''; // Acl prefix for Elasticsearch types
  this.refresh = 'refresh' in params ? params.refresh : false; // Elasticsearch refresh option for write operation, should be set to true for Acl Tests
  this.size = 'size' in params ? params.size : 1000; // Elasticsearch search size limit, to bypass default value (10)
}

ElasticSearchBackend.prototype = {
  /**
   Begins a transaction.
   */
  begin: function () {
    // returns a transaction object(just an array of functions will do here.)
    return [];
  },

  /**
   Ends a transaction (and executes it)
   */
  end: function (transaction, cb) {
    contract(arguments).params('array', 'function').end();
    async.series(transaction, function (err) {
      cb(err instanceof Error ? err : undefined);
    });
  },

  /**
   Cleans the whole storage.
   */
  clean: function (cb) {
    contract(arguments).params('function').end();
    this.client.indices.delete({
      index: this.index
    }, cb);
    //cb(undefined, true);
  },

  /**
   * get ALL documents in bucket matching keys
   * @private
   */
  _search : function(bucket, keys, cb, size){

    contract(arguments)
      .params('string', 'array|string|number', 'function')
      .end();

    var _this = this;

    var type = _this.prefix + bucket;
    var query = makeQuery('key', keys);

    _this.client.search({
      index: _this.index,
      type: type,
      ignore: [404],
      body: query,
      size: 1000
    }, function (err, response) {
      if (err) {
        return cb(error);
      }
      var data = [];
      if ('hits' in response && 'hits' in response.hits && Array.isArray(response.hits.hits) && response.hits.hits.length > 0) {
        if ('total' in response.hits && response.hits.total > response.hits.hits.length) {
          return _this._search(bucket, keys, cb, response.hits.total);
        }
        data = response.hits.hits.map(function (hit) {
          return hit._source.value;
        });
      }
      return cb(undefined, data);
    });
  },

  /**
   Gets the contents at the bucket's key.
   */
  get: function (bucket, key, cb) {

    contract(arguments)
      .params('string', 'string|number', 'function')
      .end();

    return this._search(bucket, key, cb);
  },

  /**
   Returns the union of the values in the given keys.
   */
  union: function (bucket, keys, cb) {

    contract(arguments)
      .params('string', 'array', 'function')
      .end();

    return this._search(bucket, keys, cb);
  },

  /**
   Adds values to a given key inside a bucket.
   */
  add: function (transaction, bucket, key, values) {

    contract(arguments)
      .params('array', 'string', 'string|number', 'string|array|number')
      .end();

    if (key == "key") throw new Error("Key name 'key' is not allowed.");

    var _this = this;

    transaction.push(function (cb) {

      values = makeArray(values);

      var body = [];
      values.forEach(function (value) {
        body.push({create: {_index: _this.index, _type: _this.prefix + bucket, _id: key + '-' + value}});
        body.push({key: key, value: value});
      });

      _this.client.bulk({
        refresh: _this.refresh,
        ignore: [409],
        body: body
      }, function (err, resp) {
        if (err instanceof Error) return cb(err);
        return cb(undefined);
      });

    });
  },

  /**
   Delete the given key(s) at the bucket
   */
  del: function (transaction, bucket, keys) {

    contract(arguments)
      .params('array', 'string', 'string|array')
      .end();

    var _this = this;
    keys = makeArray(keys);

    transaction.push(function (cb) {

      if (_.isEmpty(keys)) return cb(undefined);

      _this.client.deleteByQuery({
        refresh: _this.refresh,
        index: _this.index,
        type: _this.prefix + bucket,
        body: makeQuery('key', keys),
        ignore: [404]
      }, function (err) {
        if (err instanceof Error) return cb(err);
        cb(undefined);
      });

    });
  },

  /**
   Removes values from a given key inside a bucket.
   */

  remove: function (transaction, bucket, key, values) {

    contract(arguments)
      .params('array', 'string', 'string|number', 'string|array|number')
      .end();

    var _this = this;

    values = makeArray(values);

    var body = [];
    values.forEach(function (value) {
      body.push({ delete: { _index: _this.index, _type: _this.prefix + bucket, _id: key + '-' + value } });
    });

    transaction.push(function (cb) {

      _this.client.bulk({
        refresh: _this.refresh,
        body: body,
        ignore: [404]
      }, function (err) {
        if (err instanceof Error) return cb(err);
        cb(undefined);
      });

    });
  }
};

function makeQuery(field, arr) {
  arr = makeArray(arr);
  var body = {
    "query" : {
      "filtered" : {
        "filter" : {
          "bool" : {
            "should" : []
          }
        }
      }
    }
  };
  arr.forEach(function (e) {
    var term = {term: {}};
    term.term[field] = e;
    body.query.filtered.filter.bool.should.push(term);
  });
  return body;
}

function makeArray(arr) {
  return Array.isArray(arr) ? arr : [arr];
}

exports = module.exports = ElasticSearchBackend;
