var LinkedArray = function(Schema, self, values, name, rule, dirtyFields) {  
  var self = this;
  var array = Array.isArray(values[name]) ? values[name] : [];
  
  // Fetch the schema
  var schema = rule.type.schema;
  var types = rule.type.types;
  var keys = Object.keys(schema.foreign_fields);
  // Type and field
  var type = rule.type.schema;
  var field = schema.foreign_fields[name];
  // The relevant collection
  var collection = Schema.default_db.collection(type.in.collection);

  // Push an element into the array
  this.push = function(element) {    
    // Get the field mapping for this linked instance
    var field = schema.foreign_fields[name];
    
    // Set the internal field linking them
    element[field.child_foreign_id_field] = values[field.parent_id_field];

    // Push the information about the field
    dirtyFields.push({
        op: '$push_linked'
      , list_name: name
      , element: element
      , rule: rule
      , foreign_field: schema.foreign_fields[name]
    });        

    // Push to the list
    array.push(element);
  }

  this.clear = function(callback) {
    // Set up the selector
    var selector = {};
    selector[field.child_foreign_id_field] = values._id;
    // Remove all the elements
    collection.remove(selector, callback);
  }

  var findOptions = function(_options) {
    return {
      skip: function(skip) {
        _options.skip = skip;
        return findOptions(_options);
      },
      
      limit: function(limit) {   
        _options.limit = limit;
        return findOptions(_options);     
      },

      load: function(callback) {        
        // Set up the query
        var query = _options.selector;
        query[field.child_foreign_id_field] = values[field.parent_id_field];
        
        // Set the query
        var cursor = collection.find(query);

        // Apply limit and skip if needed
        if(_options.limit) cursor.limit(_options.limit);
        if(_options.skip) cursor.skip(_options.skip);

        // Execute the query
        cursor.toArray(function(err, docs) {
          if(err) return callback(err, null);
          // Let's save the array items
          for(var i = 0; i < docs.length; i++) {
            docs[i] = new rule.type(docs[i], {   
                dirtyFields: dirtyFields 
              , parent: name
              , embedded: false
              , array: self
              , index: (rule.skip + i)
            })

            // Save the document in the internal array
            array[rule.skip + i] = docs[i];
          }

          // Let's callback
          callback(null, docs);
        });
      }
    }
  }

  this.find = function(selector) {
    selector = selector || {};
    var rule = {selector: selector};
    return findOptions(rule);
  }

  this.get = function(index, callback) {
    if(!callback) throw new Error("LinkedArray requires a callback as operations are async");

    // Check if we have the current indexed item (if we preloaded)
    if(array[index]) return callback(null, array[index]);

    // Peform the lookup and cache the document
    var query = {};
    query[field.child_foreign_id_field] = values[field.parent_id_field];

    // Locate the element
    collection.findOne(query, {sort: {_id: 1}, skip:index}, function(err, doc) {
      if(err) return callback(err, null);
      if(!doc) return callback(err, null);
      // Instantiate object
      var object = new rule.type(doc, {   
            dirtyFields: dirtyFields 
          , parent: name
          , embedded: false
          , array: self
          , index: index
        });      
      // Cache the item
      array[index] = object;      
      // Return the wrapped document
      callback(null, object);
    });
  }

  this.length = function(callback) {
    var query = {};
    query[field.child_foreign_id_field] = values[field.parent_id_field];
    // Execute the count query
    collection.count(query, callback)
  }

  this.toJSON = function() {
    return array;
  }
}

exports.LinkedArray = LinkedArray;