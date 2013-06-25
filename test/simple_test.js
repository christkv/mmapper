var connect = require('../lib/mapper')
  , MongoClient = require('mongodb').MongoClient
  , Schema = connect.Schema;

exports.setUp = function(callback) {
  MongoClient.connect('mongodb://localhost:27017/mapper_test', function(err, db) {
    db.dropCollection('users', function(err) {
      db.close();
      callback();
    });
  });
}

exports.tearDown = function(callback) {
  callback();
}

exports['Should Correctly Save and change item'] = function(test) {
  // Create reusable custom type for a string
  var StringType = Schema.DefineType
    .of(String)
    .minimum.length(0)
    .maximum.length(255);

  // Create reusable custom type for a password
  var PasswordType = Schema.DefineType
    .of(String)
    .minimum.length(64); 

  // Define a User schema
  var User = Schema(function(r) {
    // Map to collection
    r.in.collection('users');
    // First name is string type definition
    r('first_name').of(StringType);
    // Last name is string type definition
    r('last_name').of(StringType)
    // Password is a password type definition
    r('password').of(PasswordType)
  });

  // Connect
  connect('mongodb://localhost:27017/mapper_test', function(err, mapper) {
    var user = new User({
        first_name: 'ole'
      , last_name: 'hansen'
      , password: 'abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd'
    });

    // Ensure basic fields set
    test.equal('ole', user.first_name);
    test.equal('hansen', user.last_name);
    test.equal('abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd', user.password);

    // Save the data
    user.save(function(err, user1) {
      test.equal(null, err);
      // Ensure basic fields set
      test.equal('ole', user1.first_name);
      test.equal('hansen', user1.last_name);
      test.equal('abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd', user1.password);

      // Modify a field and save
      user1.last_name = 'johnsen';
      user1.save(function(err, user2) {
        test.equal(null, err);

        mapper.close();
        test.done();
      });
    });
  });
}