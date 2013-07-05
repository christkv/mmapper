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

  this.load = function(callback) {
    if(callback) callback(null, null);
  }

  this.get = function(index, callback) {
    if(!callback) throw new Error("LinkedArray requires a callback as operations are async");

    var query = {};
    query[field.child_foreign_id_field] = values[field.parent_id_field];
    // var sort = {_id: 1};
    // var limi
    // Locate the element
    collection.findOne(query, {sort: {_id: 1}, skip:index}, function(err, doc) {
      if(err) return callback(err, null);
      if(!doc) return callback(err, null);
      callback(null, new rule.type(doc, {   
            dirtyFields: dirtyFields 
          , parent: name
          , embedded: true
          , array: self
          , index: index
        }));
    });

    // // Convert to basic BSON document
    // var object = array[index].toBSON ? array[index].toBSON() : array[index];
    // var instance = new rule.type(object, {   
    //           dirtyFields: dirtyFields 
    //         , parent: name
    //         , embedded: true
    //         , array: self
    //         , index: index
    //       });

    // if(callback) return callback(null, instance);
    // throw new Error("No callback provided");
  }

  this.length = function(callback) {
    var query = {};
    query[field.child_foreign_id_field] = values[field.parent_id_field];
    // Execute the count query
    collection.count(query, callback)
  }
}

exports.LinkedArray = LinkedArray;