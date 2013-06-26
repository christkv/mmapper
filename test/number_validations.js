var connect = require('../lib/mapper')
  , MongoClient = require('mongodb').MongoClient
  , Schema = connect.Schema;

exports['tearDown'] = function(callback) {
  MongoClient.connect('mongodb://localhost:27017/mapper_test', function(err, db) {
    db.dropCollection('users', function(err) {
      db.close();
      callback();
    });
  });
}

exports['tearDown'] = function(callback) {
  callback();
}

//
// Tests
//
exports['Should correctly handle number validations'] = function(test) {
  test.done();
}