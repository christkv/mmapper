var format = require('util').format;

// Returns a schema type
var define_type_schema = function(_rule) {
  // The actual type we are building on
  var define_type = {
    of: function(type) {
      // Set the type
      _rule.type = type;
      
      // Decorate the type
      if(type == String) {
        StringType(_rule.spec, define_type);
      }
      
      // Return the type again now decorated with
      // the possible options
      return define_type;
    },

    on: {
      validate: function(func) {
        if(_rule.spec.validate) 
          console.error("overriding existing validate command on DefineType");
        
        // Set the current validate function
        _rule.spec.validate = func;
      }
    },

    extend: {
      value: {
        with: {
          function: function(name, func) {
            if(_rule.spec.functions[name]) 
              console.error(format("overriding existing function %s on DefineType", name));

            _rule.spec.functions[name] = func;
          }
        }
      }
    },

    // Transformations on custom type definitions
    transform: {
      before: {
        save: function(func) {
          _rule.spec.transform.before.save[0] = func;
        },
 
        create: function(func) {
          _rule.spec.transform.before.create[0] = func;
        },
        
        update: function(func) {
          _rule.spec.transform.before.update[0] = func;
        }
      }
    }
  }

  // Return the schema
  return define_type;
}

var Rule = function() {
  this.chain = null;
  this.spec = {
    functions: {}
    // Transformation methods
    , transform: { 
        // Before events
        before: {
          // Before the save action
            save: []
          , create: []
          , update: []
        }
      }
  }

  this.extend = function(specification) {
    // Create a new rule
    var rule = new Rule();
    // Let's define the rule
    specification(define_type_schema(rule));
    // Save the extended rule as part of the chain
    rule.chain = this;
    return rule;
  }
}

var DefineType = function(specification) {
  // Create a new rule
  var rule = new Rule();
  // Let's get the rule setup
  specification(define_type_schema(rule));
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