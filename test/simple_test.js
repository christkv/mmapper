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

// Create reusable custom type for a string
var StringType = Schema.DefineType
  .of(String)
  .minimum.length(0)
  .maximum.length(255);

// Create reusable custom type for a password
var PasswordType = Schema.DefineType
  .of(String)
  .minimum.length(64); 

exports['Should Correctly Save and change item'] = function(test) {
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

        test.equal('ole', user2.first_name);
        test.equal('johnsen', user2.last_name);
        test.equal('abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd', user2.password);

        // Find a single user based on it
        User.findOne({_id: user2._id}, function(err, user3) {
          test.equal('ole', user3.first_name);
          test.equal('johnsen', user3.last_name);
          test.equal('abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd', user3.password);

          mapper.close();
          test.done();
        });
      });
    });
  });
}

exports['Should Correctly Save embedded class and retrieve it'] = function(test) {
  console.log("--------------------------------- 0")
  var Address = Schema(function(r) {
    r.embedded.in.collection('users').as.array('addresses');
    // First name is string type definition
    r('street').of(String);
  });
  console.log("--------------------------------- 1")

  // Define a User schema
  var User = Schema(function(r) {
    // Map to collection
    r.in.collection('users');
    // One or More addresses
    r('addresses').embedded.array.of(Address);
  });
  console.log("--------------------------------- 2")

  // Connect
  connect('mongodb://localhost:27017/mapper_test', function(err, mapper) {
    console.log("-------------------------------- 3")
    // Create a new user
    var user = new User({
      addresses: [
          new Address({street:"5th ave"})
        , new Address({street:"10th ave"})
      ]
    });

    // Check if we have the right amount of addresses
    test.equal(2, user.addresses.length);
    // Save the user
    user.save(function(err, user1) {
      console.log("------------------------------------")
      console.dir(err)
      console.dir(user1)
      test.equal(2, user1.addresses.length);
      test.equal('5th ave', user1.addresses.get(0).street);
      test.equal('10th ave', user1.addresses.get(1).street);

      console.log("-------------------------------------- SET")
      // Change an address
      user1.addresses.get(0).street = '20th ave';
      // Save the change
      user1.save(function() {
        mapper.close();
        test.done();
      });
    });
  });
}