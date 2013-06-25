var Schema = require('./schema')
  , MongoClient = require('mongodb').MongoClient;

// Store default db
var default_db;

// method returned
var mapper_instance = {
  close: function() {
    if(default_db) default_db.close();
  }
}

// Connect
var connect = function(url, options, callback) {
  if(typeof options == 'function') {
    callback = options;
    options = {};
  }

  MongoClient.connect(url, options, function(err, db) {
    if(err) return callback(err);
    // Set the default db
    default_db = db;
    // Set as default db
    Schema.default_db = default_db;
    // Call back
    callback(null, mapper_instance);
  });
}

connect.Schema = Schema;
// Export entire ODM functionality
module.exports = connect;


