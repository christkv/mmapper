var DefineType = {
  of: function(type) {
    // Rule
    var rule = {type: type};
    // Return the possible function
    return chainable(rule);
  }
}

//
// Chainable calls for Define type
var chainable = function(rule) {
  rule.minimum = {
    length: function(number) {
      // console.log("----------------------- minimum")
      // console.dir(rule)
      if(!rule.length) rule.length = {}
      rule.length.minimum = number;
      return chainable(rule);
    }    
  }

  rule.maximum = {
    length: function(number) {
      // console.log("----------------------- maximum")
      // console.dir(rule)
      if(!rule.length) rule.length = {}
      rule.length.maximum = number;
      return chainable(rule);
    }
  }

  return rule;
}

exports.DefineType = DefineType;