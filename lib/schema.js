var DefineType = require('./define_type').DefineType
  , ObjectID = require('mongodb').ObjectID
  , EmbeddedArray = require('./embedded_array').EmbeddedArray
  , format = require('util').format;

var Schema = module.exports = function(specification) {
  var schema_rules = {fields:{}};  

  // Define schema fields
  var schema = function(field) {
    return {
      of: function(type) {  
        schema_rules.fields[field] = {type: type};
      },

      embedded: {
        array: {
          of: function(type) {
            schema_rules.fields[field] = {type: type, embedded:true}
          }
        }
      }
    }
  }

  schema.in = {
    collection: function(collection) {
      schema_rules.in = {collection: collection};
    }    
  }

  schema.embedded = {
    in: {
      collection: function(collection) {
        schema_rules.in = {collection: collection, embedded: true};
        return {
          as: {
            array: function(array_field) {
              schema_rules.in.array_field = array_field;
            }
          }
        }
      }
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
  var SchemaObject = function(values, options) {
    var self = this;
    options = options || {};
    // Contains all the dirty fields
    var dirtyFields = options.dirtyFields || [];
    // Just check if required fields exist
    // Validation happens later
    for(var name in rules.fields) {
      if(!values[name]) throw new Error(format("Missing required field %s", name));

      // For each defined property set up an accessor
      set_up_property(this, values, name, rules.fields[name], dirtyFields, rules, options);
    } 

    // Save function
    self.save = function(callback) {
      if(!Schema.default_db) throw new Error("no db connection found");
      // console.log("------------------------ save")
      // console.dir(dirtyFields)

      self.validate(function(err) {
        if(err) return callback(err);
        // console.log("------------------------ save 1")

        // New document (no _id field)
        if(dirtyFields.length == 0 
          && !values._id) {
            // console.log("------------------------ insert")
            // Set up the _id value
            values._id = new ObjectID();
            // Add the _id field to the object
            set_up_property(self, values, '_id', null, rules, options);
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
          executeUpdate(self, values, dirtyFields, collection, options, function(err, result) {
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
        // console.log("++++++++++++++++++ validate rule :: " + key)
        // Validate the key
        validate_rule(self, key, value, rules.fields[key], function(err) {
          number_to_validate = number_to_validate - 1;
        // console.log("++++++++++++++++++ validate rule 2 :: " + key)
          
          if(err) errors.push(err, false);
          if(number_to_validate == 0 
            && errors.length == 0) return callback(null, true);
          if(number_to_validate == 0) callback(null, true);
        })
      }
    }

    self.toBSON = function() {
      // Add ObjectID if it does not exist
      if(!values._id) values._id = new ObjectID();
      // Return the value to be serialized
      return values;
    }
  }

  // FindOne method for the Schema object
  SchemaObject.findOne = function(selector, options, callback) {
    if(typeof options == 'function') {
      callback = options;
      options = {};
    }

    // console.log("--------------------- findOne")
    // console.dir(rules)

    var collection = Schema.default_db.collection(rules.in.collection);
    var projection = {};
    // If it's an embedded field, we need to rewrite the selector    
    if(rules.in.embedded && rules.in.array_field) {
      var _selector = {}
      // Rewrite the query
      for(var name in selector) {
        _selector[rules.in.array_field + "." + name] = selector[name];
      }
      // Set the projection
      projection[rules.in.array_field] = {$elemMatch: selector};
      // Set the new selector
      selector = _selector;
    }

    // console.log("========================= execute selector")
    // console.dir(selector)
    // console.dir(projection)
    // Execute the findOne
    collection.findOne(selector, projection, function(err, doc) {
      // console.log("========================= execute selector result")
      // console.dir(err)
      // console.dir(doc)
      if(err) return callback(err);
      if(!doc) return callback(err, null);
      // Return document 
      var return_doc = null;
      // If it's embedded
      if(rules.in.embedded && rules.in.array_field) {
        return_doc = doc[rules.in.array_field][0];
      } else {
        return_doc = doc;
      }

      // Return the document
      return callback(null, new SchemaObject(return_doc, {parent: doc._id}));
    });
  }

  return SchemaObject;
}

//
// Execute the update
var executeUpdate = function(self, values, dirtyFields, collection, options, callback) {
  options = options || {};
  // If this is an embedded object use the parent id in the selector
  var selector = {_id: options.parent ? options.parent : values._id};
  var update = {};

  // Build up the update
  while(dirtyFields.length > 0) {
    var field = dirtyFields.pop();

    // Simple field set
    if(field.op == '$set') {
      if(!update['$set']) update['$set'] = {};
      update['$set'][field.name] = field.value;
    } else if(field.op == '$set_in_a') {
      if(!update['$set']) update['$set'] = {};
      selector[field.parent + "._id"] = field._id;
      update['$set'][field.parent + ".$." + field.name ] = field.value;
    }
  }

  // console.log("---------- executeUpdate --------")
  // console.dir(options)
  // console.dir(selector)
  // console.dir(update)

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
  // console.log("=========================== validate_rule")
  // console.dir(rule)
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
  } else if(rule.embedded && self[name] instanceof EmbeddedArray) {
    self[name].validate(function(err, result) {
      if(err) return callback(err);
      return callback(null, null);
    });
  }
}

var set_up_property = function(self, values, name, rule, dirtyFields, rules, options) {
  if(Array.isArray(values[name])) 
    return set_up_array_property(self, values, name, rule, dirtyFields, rules, options);

  Object.defineProperty(self, name, {
    get: function() {
      return values[name];
    },

    set: function(value) {
      // console.log("========================== set_up_property")
      // console.dir(rules)
      // Set as dirty field if it's an existing document (_id exists)
      if(values._id && options.embedded && options.array) {
        dirtyFields.push({
            op: '$set_in_a'
          , name:name
          , value: value
          , _id: values._id
          , parent:options.parent
          , index: options.index
        });
      } else if(values._id && rules.in.embedded && rules.in.array_field) {
        dirtyFields.push({
            op: '$set_in_a'
          , name:name
          , value: value
          , _id: values._id
          , parent: rules.in.array_field
          , index: 0
        });        
      } else if(values._id) {
        dirtyFields.push({op: '$set', name: name, value: value});
      }
      // Set the value
      values[name] = value;
    }
  });
}

var set_up_array_property = function(self, values, name, rule, dirtyFields, options) {
  if(rule.embedded) {
    var embedded_array = new EmbeddedArray(self, values, name, rule, dirtyFields);
    // Set up the embedded_array
    Object.defineProperty(self, name, {
      get: function() {
        return embedded_array;
      },
    });    
  }
}














// Export the Define type object
module.exports.DefineType = DefineType;