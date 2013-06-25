var DefineType = require('./define_type').DefineType
  , ObjectID = require('mongodb').ObjectID
  , format = require('util').format;

var Schema = module.exports = function(specification) {
  var schema_rules = {fields:{}};  

  // Define schema fields
  var schema = function(field) {
    return {
      of: function(type) {  
        schema_rules.fields[field] = type;
      }
    }
  }

  schema.in = {
    collection: function(collection) {
      schema_rules.in = {collection: collection};
    }    
  }

  // Build the specification
  specification(schema);
  // Build the schema instance
  return buildSchemaObject(schema_rules);
}

// Build the actual schema instance
var buildSchemaObject = function(rules) {
  // Object to build
  var SchemaObject = function(values) {
    var self = this;
    // Contains all the dirty fields
    var dirtyFields = [];
    // Just check if required fields exist
    // Validation happens later
    for(var name in rules.fields) {
      if(!values[name]) throw new Error(format("Missing required field %s", name));

      // For each defined property set up an accessor
      set_up_property(this, values, name, rules.fields[name], dirtyFields);
    }    

    self.save = function(callback) {
      if(!Schema.default_db) throw new Error("no db connection found");

      self.validate(function(err) {
        if(err) return callback(err);

        // New document (no _id field)
        if(dirtyFields.length == 0 
            && !values._id) {
            // Set up the _id value
            values._id = new ObjectID();
            // Add the _id field to the object
            set_up_property(self, values, '_id', null);
            // Insert the document
            var collection = Schema.default_db.collection(rules.in.collection);
            // Insert the document
            collection.insert(values, function(err, result) {
              if(err) return callback(err);
              callback(null, self);
            });
        } else {
          // Insert the document
          var collection = Schema.default_db.collection(rules.in.collection);
          // Compile the update statement and perform the
          // update
          executeUpdate(self, values, dirtyFields, collection, function(err, result) {
            if(err) return callback(err);
            callback(null, self);
          });
        }
      });
    }

    // Validate is using a callback to support both
    // sync and async validations
    self.validate = function(callback) {
      if(!Schema.default_db) throw new Error("no db connection found");

      var errors = [];
      var keys = Object.keys(rules.fields);
      var number_to_validate = keys.length;
      // Validate the values against the rules
      for(var i = 0; i < keys.length; i++) {
        var key = keys[i];
        var value = values[key];
        // Validate the key
        validate_rule(self, key, value, rules.fields[key], function(err) {
          number_to_validate = number_to_validate - 1;
          
          if(err) errors.push(err, false);
          if(number_to_validate == 0 
            && errors.length == 0) return callback(null, true);
          if(number_to_validate == 0) callback(null, true);
        })
      }
    }
  }

  return SchemaObject;
}

//
// Execute the update
var executeUpdate = function(self, values, dirtyFields, collection, callback) {
  var selector = {_id: values._id};
  var update = {};

  // Build up the update
  while(dirtyFields.length > 0) {
    var field = dirtyFields.pop();

    if(field.op = '$set') {
      if(!update['$set']) update['$set'] = {};
      update['$set'][field.name] = field.value;
    }
  }

  // Execute the update
  collection.update(selector, update, function(err, result) {
    if(err) return callback(err);
    if(result == 0) return callback(new Error(format("Failed to update record with _id %s", values._id)));
    return callback(null, null);
  });
}

//
// Validate the rules
var validate_rule = function(self, name, value, rule, callback) {
  // Basic types  
  if(rule.type == String) {
    if(!(typeof value == 'string'))
      return callback({
          name: name
        , err: new Error(format("field %s cannot be null", name))
        , rule: rule
      });

    if(rule.length && rule.length.minimum) {
      if(value.length < rule.length.minimum)
        return callback({
            name: name
          , err: new Error(format("field %s is shorter than %s characters", name, rule.length.minimum))
          , rule: rule
        });
    }

    if(rule.length && rule.length.maximum) {
      if(value.length < rule.length.maximum)
        return callback({
            name: name
          , err: new Error(format("field %s is longer than %s characters", name, rule.length.maximum))
          , rule: rule
        });
    }

    // No error return
    return callback(null, null);
  }
}

var set_up_property = function(self, values, name, rule, dirtyFields) {
  Object.defineProperty(self, name, {
    get: function() {
      return values[name];
    },

    set: function(value) {
      // Set as dirty field if it's an existing document (_id exists)
      if(values._id)
        dirtyFields.push({op: '$set', name: name, value: value});
      // Set the value
      values[name] = value;
    }
  });
}














// Export the Define type object
module.exports.DefineType = DefineType;