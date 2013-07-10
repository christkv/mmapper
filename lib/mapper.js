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
    // console.log("---------------------------- connect to schema")
    // console.dir(Schema.types)

    // All ensure index statements that need to happen
    var indexes = [];
    var types = Schema.types;
    var keys = Object.keys(types);
    // Build all the types
    var totaltypes = keys.length;
    if(totaltypes == 0) return callback(null, mapper_instance);

    // Go over all the types
    for(var i = 0; i < keys.length; i++) {
      create_indexes(db, types[keys[i]], function(err, result) {
        totaltypes = totaltypes - 1;

        if(totaltypes == 0) {
          callback(null, mapper_instance);
        }
      });
    }
  });
}

var create_indexes = function(db, type, callback) {
  var totalindexes = type.indexes.length;
  if(type.indexes.length == 0) return callback(null, null);

  for(var i = 0; i < type.indexes.length; i++) {
    create_index(db, type, type.indexes[i], function(err, result) {
      totalindexes = totalindexes - 1;

      if(totalindexes == 0) {
        callback(null, null);
      }
    })
  }
}

var create_index = function(db, type, index, callback) {
  if(index.type == 'ttl') {
    // Build selector
    var selector = {};
    selector[index.field] = index.sort;
    var background = index.background || true;
    // Create the index
    db.collection(type.in.collection).ensureIndex(selector, {
        expireAfterSeconds: index.value
      , background: background }
      , callback);
  }
}

connect.Schema = Schema;
// Export entire ODM functionality
module.exports = connect;


