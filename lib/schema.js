var DefineType = require('./define_type').DefineType
  , ObjectID = require('mongodb').ObjectID
  , EmbeddedArray = require('./embedded_array').EmbeddedArray
  , LinkedArray = require('./linked_array').LinkedArray
  , inherits = require('util').inherits
  , format = require('util').format;

var toValidationErrors = function(err, name, rule, options) {
  if(err && Array.isArray(err)) {
    return err.map(function(value) {
      return toValidationError(name, rule, value, options);
    });
  } else if(err) {
    return [toValidationError(name, rule, err, options)];
  }

  return [];
}

var toValidationError = function(field, rule, error, options) {
  error.name = 'ValidationError';
  error.field = field;
  error.rule = rule;
  error.options = options;
  return error;
}

var Schema = module.exports = function(schema_name, specification) {
  console.log("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ DEFINE::" + schema_name)
  var schema_rules = {
      name: schema_name
    , fields:{}
    , generated_fields: []
    , foreign_fields: {}
    , extensions: []
    , indexes: []
    , transform: {
      before: { 
        create: [], update: [], remove: []
      }      
    }
    , validate: {
        before: { 
          create: [], update: [], remove: []
        }
      }
  };  

  // Store the schema rule under the name
  if(!Schema.types) Schema.types = {};
  // Add to the type
  Schema.types[schema_name] = schema_rules;

  // Let's figure out the type
  var un_chain_types = function(type) {
    var types = [type.chain, type];
    var pointer = type.chain;

    while(pointer != null) {
      pointer = pointer.chain;
      if(pointer) types.unshift(pointer);
    }

    // Return the types array
    return types;
  }

  var create_schema = function(_schema_rules) {
    // Define schema fields
    var schema = function(field, transient) {
      return {
        of: function(type) {
          // Register a chained type
          if(type.chain) {
            _schema_rules.fields[field] = un_chain_types(type);
            if(transient) _schema_rules.fields[field].transient = true;
            return;
          }

          // Register a normal type
          _schema_rules.fields[field] = type.type ? {type: type.type, spec:type.spec} : {type:type};
          if(transient) _schema_rules.fields[field].transient = true;
        },

        generated: {
          by: function(type) {
            _schema_rules.fields[field] = type.type ? {type:type.type, spec:type.spec, generated:true} : {type:type, generated:true};
          }
        },

        embedded: {
          array: {
            of: function(type) {
              _schema_rules.fields[field] = type.type 
                ? {type: type.type, spec:type.spec, embedded:true, array:true} 
                : {type: type, embedded:true, array:true}
              // Return further specialization
              return {
                with: function(cardinality) {
                  // legal cardinalities
                  var re = /[\d+\:[\d+|n]]|\d+/i;
                  // If it does not match
                  if(!re.test(cardinality)) throw new Error("Cardinality must be of form Number|Number:Number|Number:n");
                  // Split it up
                  var parts = cardinality.split(":");
                  // Minimum size
                  var min = parseInt(parts[0], 10);
                  var max = 0;
                  // If we have min numbers
                  if(min > 0) {
                    _schema_rules.fields[field].min = min;
                  }

                  // We have a max
                  if(parts.length == 2) {
                    if(parts[1].match(/\d+/)) {
                      max = parseInt(parts[1], 10);
                    }
                  }

                  // We have a maximum cardinality
                  if(max > 0) {
                   _schema_rules.fields[field].max = max; 
                  }
                }
              }
            }
          }
        },

        linked: {
          array: {
            of: function(type) {
              _schema_rules.fields[field] = type.type 
                ? {type: type.type, spec:type.spec, linked:true, array:true}
                : {type: type, linked:true, array:true}
            }
          }
        }
      }
    }

    // Added the field
    schema.field = schema;
    
    // Transient field
    schema.transient = {
      field: function(field) {
        return schema(field, true);
      }
    }

    schema.in = {
      collection: function(collection) {
        _schema_rules.in = {collection: collection};
      }    
    }

    schema.collection = {
      has: {
        ascending: {
          ttl: {
            index: function(field, timeout) {
              _schema_rules.indexes.push({type:'ttl', field:field, value:timeout, sort:1});
            }
          }        
        },

        descending: {
          ttl: {
            index: function(field, timeout) {
              _schema_rules.indexes.push({type:'ttl', field:field, value:timeout, sort:-1});
            }
          }        
        }
      }
    }

    schema.extend = {
      this: {
        with: function(method, func) {
          _schema_rules.extensions.push({method: method, func: func});
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
                      _schema_rules.foreign_fields[parent_container_field] = {
                          type: type
                        , parent_id_field: parent_id_field
                        , parent_container_field: parent_container_field
                        , child_foreign_id_field: child_foreign_id_field
                      }

                      return {
                        exposed: {
                          as: function(field) {
                            _schema_rules.foreign_fields[parent_container_field].field = field;
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

    // 
    // Add transformations
    //
    schema.transform = {
      before: {
        create: function(func) {
          _schema_rules.transform.before.create.unshift({func: func});
        }, 

        update: function(func) {
          _schema_rules.transform.before.update.unshift({func: func});
        }, 

        remove: function(func) {
          _schema_rules.transform.before.remove.unshift({func: func});
        }, 
      }
    }  

    //
    // Let's you hook up custom object level validation methods
    //
    schema.validate = {
      //
      // Synchronous validators (f.ex comparing internal values like password/confirm password)
      //
      before: {
        create: function(func) {
          _schema_rules.validate.before.create.push({func: func});
        },

        update: function(func) {
          _schema_rules.validate.before.update.push({func: func});
        },

        remove: function(func) {
          _schema_rules.validate.before.update.push({func: func});
        }
      },

      //
      // Asynchronous validators (calling out f.ex to other code)
      //
      async: {
        before: {
          create: function(func) {
            _schema_rules.validate.before.create.push({func: func, async: true});
          },

          update: function(func) {
            _schema_rules.validate.before.update.push({func: func, async: true});
          },

          remove: function(func) {
            _schema_rules.validate.before.update.push({func: func, async: true});
          }
        }
      }
    }

    schema.embedded = {
      in: {
        collection: function(collection) {
          _schema_rules.in = {collection: collection, embedded: true};
          return {
            as: {
              array: function(array_field) {
                _schema_rules.in.array_field = array_field;
              }
            }
          }
        }
      }
    }

    return schema;
  }

  // Build the specification
  specification(create_schema(schema_rules));
  
  // Aggregate up all transform methods for performance
  add_transform_methods(schema_rules);

  // Build the schema instance
  return buildSchemaObject(schema_name, schema_rules, create_schema);
}

// Map the field
var map_field = function(_schema_rules, _array_name, _field, _field_name) {
  if(_field.spec && _field.spec.transform.before[_array_name].length > 0) {
    var funcs = _schema_rules.transform.before[_array_name];
    var field_funcs = _field.spec.transform.before[_array_name];
    
    // Add all the functions
    for(var j = 0; j < field_funcs.length; j++) {
      _schema_rules.transform.before[_array_name].push({func: field_funcs[j], field: _field, name: _field_name});
    }            
  }      
}

// Add transform functions to global transform queue
var add_transform_methods = function(schema_rules) {
  // Map all the field transforms into the global list of functions
  var fields = schema_rules.fields;
  var keys = Object.keys(fields);
  
  // Go over all the fields and accumulate the transforms
  for(var i = 0; i < keys.length; i++) {
    var field = fields[keys[i]];

    // Add the transform to the main list
    // Chained type
    if(Array.isArray(field)) {
      field.forEach(function(_field) {
        map_field(schema_rules, 'create', _field, keys[i]);
        map_field(schema_rules, 'update', _field, keys[i]);
        map_field(schema_rules, 'remove', _field, keys[i]);
      });
    } else if(field.spec && field.spec.transform.before.create.length > 0) {
      map_field(schema_rules, 'create', field, keys[i]);
      map_field(schema_rules, 'update', field, keys[i]);
      map_field(schema_rules, 'remove', field, keys[i]);
    }
  }  
}

// Build the actual schema instance
var buildSchemaObject = function(schema_name, rules, schema) {
  // Object to build
  var SchemaObject = function(values, options) {
    // console.log("================================= " + schema_name)
    var self = this;
    var isNew = false;
    options = options || {};
    
    // Add our type constructor
    rules.type = SchemaObject;
    // Collection
    var _collection = Schema.default_db.collection(rules.in.collection);
    // Contains all the dirty fields
    var dirtyFields = options.dirtyFields || [];
    
    // If we have no _id field set one
    if(values._id == null) {
      values._id = new ObjectID();
      isNew = true;
    }

    // Set up _id property
    set_up_id_property(self, values);

    // Set up the properties
    for(var name in rules.fields) {
      if(!rules.fields[name].transient) {
        set_up_property(this, values, name, rules.fields[name], dirtyFields, rules, options);
      } else {
        set_up_transient_property(this, values, name, rules.fields[name], dirtyFields, rules, options);
      }
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
      
      // State of the save
      var state = isNew ? 'new' : 'update';
      
      // Validate the object
      self.validate({state: state}, function(err) {
        console.dir("== SAVE")
        console.dir(err)
        console.dir(isNew)
        if(err) return callback(err);
        // New document (no _id field)
        if(isNew) {
          // No longer a new object
          isNew = false;
          
          console.dir("== SAVE 1")
          // Apply all create level transformations
          apply_create_transforms(self, values, rules, function(err, result) {
            // Clean out dirty fields
            dirtyFields.splice(0);

            // Insert the document
            _collection.insert(values, function(err, result) {
              if(err) return callback(err);
              callback(null, self);
            });
          });
        } else if(dirtyFields.length > 0) {
          // Compile the update statement and perform the update
          executeUpdate(self, values, dirtyFields, _collection, options, function(err, result) {
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
    self.validate = function(options, callback) {
      if(!Schema.default_db) throw new Error("no db connection found");
      if(typeof options == 'function') {
        callback = options;
        options  = {};
      }

      // Holds all the state of the validation process
      var errors = [];
      var keys = Object.keys(rules.fields);
      var number_to_validate = keys.length;

      // Validate the values against the rules
      for(var i = 0; i < keys.length; i++) {
        var key = keys[i];
        var value = values[key];

        // Validate the key
        validate_rule(self, key, value, rules.fields[key], options, function(err) {
          if(err) errors = errors.concat(err);

          // Adjust the number of validation's left
          number_to_validate = number_to_validate - 1;

          // We've finished validating
          if(number_to_validate == 0) {
            apply_custom_validations(self, rules, errors, options, callback);
          }
        })
      }
    }

    self.destroy = function(callback) {
      _collection.remove({_id: self._id}, function(err, number_of_removed) {
        if(err) return callback(err, null);
        if(number_of_removed == 0) 
          return callback(new Error(format("failed to destroy %s object with _id %s", schema_name, self._id)), null);
        // Done
        return callback(null, null);
      });
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

  // Scope thing
  SchemaObject.find = function(selector) {
    return {
      updateOneAndGet: function(update, callback) {
        // Retrieve the schema collection
        var collection = Schema.default_db.collection(rules.in.collection);
        collection.findAndModify(selector, {}, update, {new:true}, function(err, doc) {
          if(err) return callback(err, null);
          if(!doc) return callback(null, null);
          return callback(null, new SchemaObject(doc, {parent: doc._id}));
        });
      }
    }
  }

  // FindOne method for the Schema object
  SchemaObject.findOne = function(selector, options, callback) {
    if(typeof options == 'function') {
      callback = options;
      options = {};
    }

    // Retrieve the schema collection
    var collection = Schema.default_db.collection(rules.in.collection);
    // Initial projection for the query
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

  SchemaObject.extend = function(new_schema_name, specification) {
    console.log("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ extend")

    // New schema rule
    var _schema_rules = {
        name: new_schema_name
      , fields:rules.fields
      , generated_fields: rules.generated_fields.slice(0)
      , foreign_fields: {}
      , extensions: rules.extensions.slice(0)
      , indexes: rules.indexes.slice(0)
      , transform: {
          before: { 
              create: rules.transform.before.create.slice(0)
            , update: rules.transform.before.update.slice(0)
            , remove: rules.transform.before.remove.slice(0)
          }      
      }
      , validate: {
          before: { 
              create: rules.validate.before.create.slice(0)
            , update: rules.validate.before.update.slice(0)
            , remove: rules.validate.before.remove.slice(0)
          }
        }
    };

    // Add the in part
    if(rules.in) {
      var _in = {};
      for(var name in rules.in) {
        _in[name] = rules.in[name];
      }
      _schema_rules.in = _in;
    }

    // Build the specification
    specification(schema(_schema_rules));
    // Build the schema instance
    return buildSchemaObject(schema_name, _schema_rules);
  }

  return SchemaObject;
}

//
// Apply custom validations
var apply_custom_validations = function(self, rules, errors, options, callback) {
  // console.log("= apply_custom_validations apply_custom_validations apply_custom_validations");
  // console.log("= apply_custom_validations apply_custom_validations apply_custom_validations");
  // console.log("= apply_custom_validations apply_custom_validations apply_custom_validations");

  // Operations to apply
  var operations = [];

  // Depending on the state
  if(options.state == 'new') {
    operations = rules.validate.before.create.slice(0);
  } else if(options.state == 'update') {
    operations = rules.validate.before.update.slice(0);
  } else if(options.state == 'remove') {
    operations = rules.validate.before.remove.slice(0);
  }

  // We've got no validations
  if(operations.length == 0) {
    if(errors.length > 0) return callback(errors, false);    
    return callback(null, true);
  }

  // Let's get moving on the validations
  var total_validations = operations.length;

  // Iterate over all the operations
  for(var i = 0; i < operations.length; i++) {
    var op = operations[i];

    if(op.async) {
      op.func.call(self, self, function(err, results) {
        total_validations = total_validations - 1;
  
        // Add any custom errors
        errors = add_custom_errors(errors, err);

        // No more validations left
        if(total_validations == 0) {
          if(errors.length > 0) return callback(errors, false);    
          return callback(null, true);
        }
      });
    } else {
      var result = op.func.call(self, self);
      total_validations = total_validations - 1;
      
      // Add any custom errors
      errors = add_custom_errors(errors, result);

      // No more validations left
      if(total_validations == 0) {
        if(errors.length > 0) return callback(errors, false);    
        return callback(null, true);
      }
    }
  }
}

// Add any errors
var add_custom_errors = function(errors, result) {
  if(Array.isArray(result)) {
    return errors.concat(result.map(function(error) { 
      if(error.name == 'ValidationError') return error;
      return toValidationError('custom', null, error, {parent:null, field:null});
    }));
  } else if(result instanceof Error) {
    if(result.name == 'ValidationError') errors.push(error);
    else errors.push(toValidationError('custom', null, result, {parent:null, field:null}));
  } 

  return errors;
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

          // Execute the update
          collection.update(selector, update, function(err, result) {
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
var validate_rule = function(self, name, value, rule, options, callback) {
  var errors = [];

  if(rule.generated) {
    callback(null, true);
  } else if(rule.spec && rule.spec.validate) {
    var result = toValidationErrors(rule.spec.validate(name, value), name, rule, options);
    callback(result, result ? false : true);
  } else if(rule.spec && rule.spec.validateAsync) {
    rule.spec.validateAsync(name, value, function(err, result) {
      var err = toValidationErrors(err, name, rule, options);
      callback(err, result);
    });
  } else if(rule.array) {
    validate_array(self, name, rule, value, options, callback);
  } else if(Array.isArray(rule)) {
    validate_array_of_rules(self, name, value, rule, options, callback);
  } else {
    callback(null, true);
  }
}

var validate_array_of_rules = function(self, name, value, rules, options, callback) {
  var validations = rules.length;
  var errors = [];

  // Go through all the validations
  for(var i = 0; i < rules.length; i++) {
    var _validate_rule = function(_self, _name, _value, _rule, _options, _callback) {
      // Validate a rule
      validate_rule(_self, _name, _value, _rule, _options, function(err) {
        validations = validations - 1;

        // Concatenate errors
        if(err) errors = errors.concat(err);

        // If no more validations we are done
        if(validations == 0) {
          callback(errors.length > 0 ? errors : null, null);
        }
      });      
    }

    // Execute the validation
    _validate_rule(self, name, value, rules[i], options, callback);
  }
}


var validate_array = function(self, name, rule, value, options, callback) {
  var array = self[name];
  // Extract the length
  array.length(function(err, length) {
    if(rule.min && length < rule.min) {
      callback(new Error(format("%s requires at least %d elements", name, rule.min)));
    } else if(rule.max && length > rule.max) {
      callback(new Error(format("%s requires at most %d elements", name, rule.max)));
    } else {
      var remaining = length;
      var errors = [];

      // We need to validate each of the elements
      for(var i = 0; i < length; i++) {
        array.get(i, function(err, doc) {
          // If we have an error save the error
          if(err) {
            remaining = remaining - 1;
            errors.push(err);
            // If it was the last element
            if(remaining == 0) {
              callback(errors.length > 0 ? errors : null, null);
            }
          } else {
            doc.validate({parent: name, index: i, state: options.state}, function(err, result) {
              remaining = remaining - 1;

              // Concatenate the errors if any
              if(Array.isArray(err)) {
                errors = errors.concat(err);
              }
  
              // If it was the last element
              if(remaining == 0) {
                callback(errors.length > 0 ? errors : null, null);
              }
            });
          }
        });
      }
    }
  });
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
  Object.defineProperty(self, '_id', {
    get: function() {
      return values['_id'];
    },
    enumerable:true
  });
}

var set_up_transient_property = function(self, values, name, rule, dirtyFields, rules, options) {
  var value = values[name] ? values[name] : null;
  delete values[name];

  // Setting up value that's not saved
  Object.defineProperty(self, name, {
    get: function() {
      return value;
    },

    set: function(_value) {
      value = _value;
    }
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
var apply_create_transforms = function(self, values, rules, callback) {
  console.log("++++++++++++++++++++++++++++++++++++++++++++ FIELDS")
  console.log("++++++++++++++++++++++++++++++++++++++++++++ FIELDS")
  console.log("++++++++++++++++++++++++++++++++++++++++++++ FIELDS")
  console.dir(rules.fields)
  // No transforms return
  if(rules.transform.before.create.length == 0)
    return callback(null, null);

  var transforms = rules.transform.before.create;
  console.log("++++++++++++++++++++++++++++++++++++++ TRANSFORMS")
  console.dir(transforms)
  var total_transforms = transforms.length;
  // Apply all transforms
  for(var i = 0; i < transforms.length; i++) {
    // Execute transform
    var f = function(_transform, _values) {
      if(_transform.field) {
        _transform.func(_values[_transform.field], function(err, value) {
          total_transforms = total_transforms - 1;
          // Set the transformed field
          _values[_transform.name] = value;
          // No more transforms finish up
          if(total_transforms == 0) 
            callback(null, null);
        });          
      } else {
        _transform.func(self, function(err, value) {
          total_transforms = total_transforms - 1;          
          // No more transforms finish up
          if(total_transforms == 0) 
            callback(null, null);
        })
      }
    };

    // Execute
    f(transforms[i], values);
  }
}

// Export the Define type object
module.exports.DefineType = DefineType;