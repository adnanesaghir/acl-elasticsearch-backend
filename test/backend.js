'use strict';

var Backend = require('../lib');
var assert = require('chai').assert;
var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  apiVersion: '1.7',
  hosts: ['localhost:9200'],
  log: 'trace',
  keepAlive: true,
  maxSockets: 100,
  minSockets: 100
});
var backend;
var custom;

describe('Backend Tests', function() {
  it('should validate mandatory parameters', function() {
    try {
      backend = new Backend();
      assert.fail('should raise an exception');
    } catch (e) {
      assert(e);
    }
    try {
      backend = new Backend({
        index: 'foo'
      });
      assert.fail('should raise an exception');
    } catch (e) {
      assert(e);
    }
    backend = new Backend({
      client: client
    });
    assert.instanceOf(backend, Backend);
  });
  it('should handle custom parameters', function() {
    custom = new Backend({
      client: client,
      index: 'foo',
      prefix: 'bar',
      refresh: true,
      size: 5
    });
    assert.instanceOf(custom, Backend);
    assert.strictEqual(custom.index, 'foo');
    assert.strictEqual(custom.prefix, 'bar');
    assert.strictEqual(custom.refresh, true);
    assert.strictEqual(custom.size, 5);
  });
});
