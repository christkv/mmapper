var format = require('util').format;

var DefineType = function(specification) {
  // Rule
  var rule = {};

  // The actual type we are building on
  var define_type = {
    of: function(type) {
      // Set the type
      rule.type = type;
      rule.spec = {};
      
      // Decorate the type
      if(type == String) {
        StringType(rule.spec, define_type);
      }
      
      // Return the type again now decorated with
      // the possible options
      return define_type;
    }    
  }

  // Let's get the rule setup
  specification(define_type);
  // Return the finished up rule
  return rule;
}

//
//  Default String Type
// 
var StringType = function(spec, define_type) {
  // The validation method (validate or validateAsync)
  spec.validate = function(field, value) {
    if(!value) return new Error(format("field %s cannot be null", field));

    if(spec.length && spec.length.minimum) {
      if(value.length < spec.length.minimum) {
        return new Error(format("field %s cannot be shorter than %s characters", field, spec.length.minimum));
      }
    }

    if(spec.length && spec.length.maximum) {
      if(value.length > spec.length.maximum) {
        return new Error(format("field %s cannot be longer than %s characters", field, spec.length.maximum));
      }
    }
  };

  // The minimum definition
  define_type.minimum = {
    length: function(number) {
      if(!spec.length) spec.length = {};
      spec.length.minimum = number;
    }
  }

  // The maximum definition
  define_type.maximum = {
    length: function(number) {
      if(!spec.length) spec.length = {};
      spec.length.maximum = number;
    }
  }
}

exports.DefineType = DefineType;