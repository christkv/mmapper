var ObjectID = require('mongodb').ObjectID;

var EmbeddedArray = function(self, values, name, rule, dirtyFields) {  
  var self = this;
  var array = Array.isArray(values[name]) ? values[name] : [];

  this.get = function(index, callback) {
    // Convert to basic BSON document
    var object = array[index].toBSON ? array[index].toBSON() : array[index];
    var instance = new rule.type(object, {   
              dirtyFields: dirtyFields 
            , parent: name
            , embedded: true
            , array: self
            , index: index
          });

    if(callback) return callback(null, instance);
    return instance;
  }

  this.load = function(callback) {
    if(callback) callback(null, null);
  }

  this.push = function(element) {
    // Push the information about the field
    dirtyFields.push({
        op: '$push'
      , name: name
      , value: element
    });        
    // Push the element to the array
    array.push(element);
  }

  this.validate = function(callback) {
    callback(null, null);
  }

  this.length = function(callback) {
    callback(null, array.length);
  }

  // Convert the representation to bson
  this.toBSON = function() {
    return array;
  }
}

exports.EmbeddedArray = EmbeddedArray;