var EmbeddedArray = function(self, values, name, rule, dirtyFields) {  
  var self = this;
  var array = Array.isArray(values[name]) ? values[name] : [];

  this.get = function(index) {
    // Convert to basic BSON document
    var object = array[index].toBSON ? array[index].toBSON() : array[index];
    // Return the new type
    return new rule.type(object
      , {   
            dirtyFields: dirtyFields 
          , parent: name
          , embedded: true
          , array: self
          , index: index
        });
  }

  this.push = function(element) {    
    array.push(element);
  }

  this.validate = function(callback) {
    callback(null, null);
  }

  Object.defineProperty(self, 'length', {
    get: function() {
      return array.length;
    },
  });

  // Convert the representation to bson
  this.toBSON = function() {
    return array;
  }
}

exports.EmbeddedArray = EmbeddedArray;