var DefineType = require('./define_type').DefineType
  , ObjectID = require('mongodb').ObjectID
  , EmbeddedArray = require('./embedded_array').EmbeddedArray
  , LinkedArray = require('./linked_array').LinkedArray
  , format = require('util').format;

var Schema = module.exports = function(schema_name, specification) {
  var schema_rules = {
      name: schema_name
    , fields:{}
    , foreign_fields: {}
    , extensions: []
    , indexes: []
  };  

  // Store the schema rule under the name
  if(!Schema.types) Schema.types = {};
  // Add to the type
  Schema.types[schema_name] = schema_rules;

  // Define schema fields
  var schema = function(field) {
    return {
      of: function(type) {
        schema_rules.fields[field] = type.type ? {type: type.type, spec:type.spec} : {type:type};
      },

      embedded: {
        array: {
          of: function(type) {
            schema_rules.fields[field] = type.type 
              ? {type: type.type, spec:type.spec, embedded:true, array:true} 
              : {type: type, embedded:true, array:true}
          }
        }
      },

      linked: {
        array: {
          of: function(type) {
            schema_rules.fields[field] = type.type 
              ? {type: type.type, spec:type.spec, linked:true, array:true}
              : {type: type, linked:true, array:true}
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

  schema.collection = {
    has: {
      ascending: {
        ttl: {
          index: function(field, timeout) {
            schema_rules.indexes.push({type:'ttl', field:field, value:timeout, sort:1});
          }
        }        
      },

      descending: {
        ttl: {
          index: function(field, timeout) {
            schema_rules.indexes.push({type:'ttl', field:field, value:timeout, sort:-1});
          }
        }        
      }
    }
  }

  schema.extend = {
    this: {
      with: function(method, func) {
        schema_rules.extensions.push({method: method, func: func});
      }
    }
  }

  schema.linked = {
    to: function(type) {
      return {
        using: function(parent_id_field) {
          return {
            through: {
              field: function(parent_container_field) {
                return {
                  as: function(child_foreign_id_field) {
                    schema_rules.foreign_fields[parent_container_field] = {
                        type: type
                      , parent_id_field: parent_id_field
                      , parent_container_field: parent_container_field
                      , child_foreign_id_field: child_foreign_id_field
                    }

                    return {
                      exposed: {
                        as: function(field) {
                          schema_rules.foreign_fields[parent_container_field].field = field;
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
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
  return buildSchemaObject(schema_name, schema_rules);
}

// Build the actual schema instance
var buildSchemaObject = function(schema_name, rules) {
  // Object to build
  var SchemaObject = function(values, options) {
    // console.log("================================= " + schema_name)
    var self = this;
    var isNew = false;
    options = options || {};
    
    // Add our type constructor
    rules.type = SchemaObject;

    // Contains all the dirty fields
    var dirtyFields = options.dirtyFields || [];
    
    // If we have no _id field set one
    if(values._id == null) {
      values._id = new ObjectID();
      isNew = true;
    }

    // Set up _id property
    set_up_id_property(self, values);

    // Just check if required fields exist
    // Validation happens later
    for(var name in rules.fields) {
      if(!values[name] && rules.fields[name].array) {

      } else if(!values[name]) {
        throw new Error(format("Missing required field %s", name));
      }

      // For each defined property set up an accessor
      set_up_property(this, values, name, rules.fields[name], dirtyFields, rules, options);
    } 

    // Set up foreign fields
    for(var name in rules.foreign_fields) {
      set_up_foreignkey(this, values, name, rules.foreign_fields[name], dirtyFields, rules, options);
    }

    // Decorate the object with custom functions
    for(var i = 0; i < rules.extensions.length; i++) {
      var extension = rules.extensions[i];
      self[extension.method] = extension.func;
    }    

    // Save function
    self.save = function(callback) {
      if(!Schema.default_db) throw new Error("no db connection found");
      // Validate the object
      self.validate(function(err) {
        if(err) return callback(err);
        // New document (no _id field)
        if(isNew) {
          // No longer a new object
          isNew = false;
          // Apply all create level transformations
          apply_create_transforms(values, rules, function(err, result) {
            // Clean out dirty fields
            dirtyFields.splice(0);
            // Insert the document
            var collection = Schema.default_db.collection(rules.in.collection);
            // Insert the document
            collection.insert(values, function(err, result) {
              if(err) return callback(err);
              callback(null, self);
            });
          });
        } else if(dirtyFields.length > 0) {
          // Insert the document
          var collection = Schema.default_db.collection(rules.in.collection);
          // Compile the update statement and perform the
          // update
          executeUpdate(self, values, dirtyFields, collection, options, function(err, result) {
            if(err) return callback(err);
            callback(null, self);
          });
        } else {
          callback(null, self);
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
          
          if(err) errors.push(err);
          if(number_to_validate == 0 
            && errors.length == 0) return callback(null, true);
          if(number_to_validate == 0 && errors.length > 0)
            return callback(errors, null);
          if(number_to_validate == 0) callback(null, true);
        })
      }
    }

    self.toBSON = function() {
      return values;
    }
  }

  Object.defineProperty(SchemaObject, 'schema', {
    get: function() {
      return rules;
    },
    enumerable:false
  });

  Object.defineProperty(SchemaObject, 'types', {
    get: function() {
      return Schema.types;
    },
    enumerable:false
  });

  // FindOne method for the Schema object
  SchemaObject.findOne = function(selector, options, callback) {
    if(typeof options == 'function') {
      callback = options;
      options = {};
    }

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

    // Execute the findOne
    collection.findOne(selector, projection, function(err, doc) {
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
// Apply any transforms
var apply_update_transforms = function(field, update, selector, values, callback) {
  // Apply the operation
  var apply_field = function(_field, _update, _selector, _value) {
    if(_field.op == '$set') {
      if(!_update['$set']) _update['$set'] = {};
      _update['$set'][_field.name] = _value;
    } else if(_field.op == '$set_in_a') {
      if(!_update['$set']) _update['$set'] = {};
      _selector[_field.parent + "._id"] = _field._id;
      _update['$set'][_field.parent + ".$." + _field.name ] = _value;
    } else if(_field.op == '$push') {
      if(!_update['$push']) _update['$push'] = {};
      _update['$push'][_field.name] = _value;
    }        
  }


  // Check if we have a transformation chain
  if(field.rule 
    && field.rule.spec
    && field.rule.spec.transform.before.update.length > 0) {
      field.rule.spec.transform.before.update[0](field.value, function(err, value) {
        values[field.name] =  value;
        // Apply the field
        apply_field(field, update, selector, value);
        // Perform callback
        callback();
      });
  } else {
    // Apply the field
    apply_field(field, update, selector, field.value);
    // Perform callback
    callback();
  }
}

//
// Execute the update
var executeUpdate = function(self, values, dirtyFields, collection, options, callback) {
  // console.log("---------- executeUpdate -------- 0")
  options = options || {};
  // If this is an embedded object use the parent id in the selector
  var selector = {_id: options.parent ? options.parent : values._id};
  var update = {};
  
  // Keep track of the number of dirty fields
  var number_of_dirty_fields = dirtyFields.length;
  // Linked instances
  var linked_update_operations = [];

  // Process all the dirty fields
  while(dirtyFields.length > 0) {
    var field = dirtyFields.pop();

    // If we have a linked_update_operation save
    if(field.op == '$push_linked') {
      // Save the value
      field.element.save(function(err, doc) {
        number_of_dirty_fields = number_of_dirty_fields - 1;

        // Execute the update statement
        if(number_of_dirty_fields == 0) {
          if(err) return callback(err);
          return callback(null, null);
        }
      });
    } else {
      
      // Apply any update transformations
      apply_update_transforms(field, update, selector, values, function() {
        number_of_dirty_fields = number_of_dirty_fields - 1;

        // Execute the update statement
        if(number_of_dirty_fields == 0) {
          // console.log("---------- executeUpdate -------- 1")
          // console.dir(values)
          // Execute the update
          collection.update(selector, update, function(err, result) {
            // console.log("---------- executeUpdate -------- 2")
            // console.dir(linked_update_operations)

            if(err) return callback(err);
            if(result == 0) return callback(new Error(format("Failed to update record with _id %s", values._id)));
            return callback(null, null);
          });        
        }
      });      
    }
  }
}

//
// Validate the rules
var validate_rule = function(self, name, value, rule, callback) {
  // console.log("=========================== validate_rule")
  // console.dir(rule)
  if(rule.spec && rule.spec.validate) {
    var result = rule.spec.validate(name, value);
    callback(result, result ? false : true);
  } else if(rule.spec && rule.spec.validateAsync) {
    rule.spec.validateAsync(name, value, callback);
  } else {
    callback(null, true);
  }
}

var set_up_foreignkey = function(self, values, name, field, dirtyFields, rules, options) {
  Object.defineProperty(self, field.child_foreign_id_field, {
    get: function() {
      return values[field.child_foreign_id_field];
    },
    set: function(value) {
      return values[field.child_foreign_id_field] = value;
    },
    enumerable:true
  }); 

  // Is this mapped back to a linked object
  if(field.field) {
    // Get the schema type
    var schema = Schema.types[field.type];
    // Set up a function that maps to the remote type
    self[field.field] = function(callback) {
      schema.type.findOne({_id: values[field.child_foreign_id_field]}, callback);
    }
  } 
}

var set_up_id_property = function(self, values) {
  // Define the property
  Object.defineProperty(self, '_id', {
    get: function() {
      return values['_id'];
    },
    enumerable:true
  });
}

var set_up_property = function(self, values, name, rule, dirtyFields, rules, options) {
  if(rule.array) 
    return set_up_array_property(self, values, name, rule, dirtyFields, rules, options);

  Object.defineProperty(self, name, {
    get: function() {
      var value = values[name];
      // Decorate the value with any functions
      if(rule.spec && Object.keys(rule.spec.functions).length > 0) {
        var new_value = {};
        var keys = Object.keys(rule.spec.functions);

        // Decorate with new functions
        for(var i = 0; i < keys.length; i++) {
          new_value[keys[i]] = rule.spec.functions[keys[i]];
        }

        // Build a wrapper object around the value
        new_value.value = value;
        new_value.define_type = rule;
        new_value.field = name;
        // Override internal value
        value = new_value;        
      }

      // Return the value
      return value;
    },

    set: function(value) {
      if(value.define_type) {
        value = value.value;
      }
      
      // Set as dirty field if it's an existing document (_id exists)
      if(values._id && options.embedded && options.array) {
        dirtyFields.push({
            op: '$set_in_a'
          , name:name
          , value: value
          , _id: values._id
          , parent:options.parent
          , index: options.index
          , rule: rule
        });
      } else if(values._id && rules.in.embedded && rules.in.array_field) {
        dirtyFields.push({
            op: '$set_in_a'
          , name:name
          , value: value
          , _id: values._id
          , parent: rules.in.array_field
          , index: 0
          , rule: rule
        });        
      } else if(values._id) {
        dirtyFields.push({
            op: '$set'
          , name: name
          , value: value
          , rule: rule
        });
      }
      
      // Set the value
      values[name] = value;
    },
    enumerable: true
  });
}

var set_up_array_property = function(self, values, name, rule, dirtyFields, options) {
  var array = null;
  if(rule.embedded) {
    array = new EmbeddedArray(self, values, name, rule, dirtyFields);
  } else if(rule.linked) {
    array = new LinkedArray(Schema, self, values, name, rule, dirtyFields);
  }

  // Set up the array
  Object.defineProperty(self, name, {
    get: function() {
      return array;
    },
    enumerable: true
  });
}

//
//  Apply any transformations on fields at create
//
var apply_create_transforms = function(values, rules, callback) {
  var keys = Object.keys(rules.fields);
  if(keys.length == 0) return callback(null, null);

  // Needed transforms
  var transforms = 0;

  // Number of transforms
  for(var i = 0; i < keys.length; i++) {
    var field = rules.fields[keys[i]];
    // If we have a before save event
    if(field.spec && field.spec.transform.before.create.length > 0) {
      transforms = transforms + field.spec.transform.before.create.length;
    }
  }

  // No transforms available
  if(transforms == 0) return callback(null, null);

  // Iterate over all the fields and execute the transforms
  for(var i = 0; i < keys.length; i++) {
    var field = rules.fields[keys[i]];
    // If we have a before save event
    if(field.spec && field.spec.transform.before.create.length > 0) {
      var create_transforms = field.spec.transform.before.create;

      // Execute the transforms
      for(var j = 0; j < create_transforms.length; j++) {        
        // Execute transform
        var f = function(_create_transforms, _field, _values) {
          _create_transforms(_values[_field], function(err, value) {
            transforms = transforms - 1;
            // Set the transformed field
            _values[_field] = value;
            // No more transforms finish up
            if(transforms == 0) {
              callback(null, null);
            }
          });          
        };

        // Execute
        f(create_transforms[j], keys[i], values);
      }
    }
  }
}

// Export the Define type object
module.exports.DefineType = DefineType;