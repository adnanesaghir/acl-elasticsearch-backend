'use strict';

var Backend = require('../lib');
var assert = require('chai').assert;
var elasticsearch = require('elasticsearch');
var Acl = require('acl');
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
      index: 'acltest',
      prefix: 'bar_',
      refresh: true,
      size: 5
    });
    assert.instanceOf(custom, Backend);
    assert.strictEqual(custom.index, 'acltest');
    assert.strictEqual(custom.prefix, 'bar_');
    assert.strictEqual(custom.refresh, true);
    assert.strictEqual(custom.size, 5);
  });
  it('should bypass ES default search size of 10 results', function(done) {
    var acl = new Acl(custom);
    custom.clean(function(err, res) {
      if (err) {
        console.error(err);
      }
      var roles = [];
      for (var i = 1; i <= 15; i++) {
        roles.push('role_' + i);
      }
      acl.addUserRoles('U1', roles, function(err) {
        if (err) {
          done(err)
        } else {
          acl.userRoles('U1', function(err, result) {
            if (err) {
              done(err);
            } else {
              assert.isOk(result.length >= 15);
              done();
            }
          });
        }
      });
    });
  });
});
