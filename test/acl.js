'use strict';

var ElasticSearchBackend = require('../lib');
var tests = require('../node_modules/acl/test/tests');
var backendTests = require('../node_modules/acl/test/backendtests');

describe('Acl Tests', function () {
  //this.timeout(7000);
  before(function (done) {
    var self = this;
    var elasticsearch = require('elasticsearch');
    var client = new elasticsearch.Client({
      apiVersion: '1.7',
      hosts: ['localhost:9200'],
      log: 'trace',
      keepAlive: true,
      maxSockets: 100,
      minSockets: 100
    });
    client.indices.putTemplate({
      name: 'acltest-template',
      body: {
        template: 'acltest',
        mappings: {
          _default_: {
            dynamic_templates: [
              {
                string_fields: {
                  match: '*',
                  match_mapping_type: 'string',
                  mapping: {
                    type: 'string',
                    index: 'not_analyzed'
                  }
                }
              }
            ]
          }
        }
      }
    }, function(err, resp) {
      if (err) {
        done(err);
      } else {
        self.backend = new ElasticSearchBackend({
          client: client,
          index: 'acltest',
          refresh: true,
        });
        done();
      }
    });
  });

  run();
});

function run() {
  Object.keys(tests).forEach(function (test) {
    tests[test]()
  });

  Object.keys(backendTests).forEach(function (test) {
    backendTests[test]()
  });
}