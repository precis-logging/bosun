var Lambda = require('lambda-30').Lambda;

var _compileObject = function(obj, options){
  var keys = Object.keys(obj), result={}, exec = [];
  keys.forEach(function(key){
    var value = obj[key], type=typeof(value);
    if(type==='string'){
      return exec.push((function(key, l){
        return function(res, source){
          res[key] = l(source);
        };
      })(key, Lambda(value, options)));
    }
    if(type==='object'){
      if(!(value instanceof Array)){
        return exec.push((function(key, f){
          return function(res, source){
            res[key] = f(source);
          };
        })(key, _compileObject(value, options)));
      }
    }
    exec.push((function(key, value){
      return function(res){
        return res[key]=value;
      };
    })(key, value));
  });
  return (function(steps){
    return function(source){
      var result = {};
      steps.forEach(function(f){
        f(result, source);
      });
      return result;
    };
  })(exec);
};

var Reform = function(rules, options){
  var self = this;
  self.options = options || {};
  self.compile(rules || {});
};

Reform.prototype.compile = function(rules, options){
  var self = this;
  var type = typeof(rules);
  if(type === 'string'){
    return self.transformer = Lambda(rules, options);
  }
  if(type === 'object'){
    return self.transformer = _compileObject(rules, options);
  }
  throw new Error('Expected string or Object, got '+type);
};

Reform.prototype.reform = function(source){
  var self = this;
  return self.transformer(source);
};

var makeReform = function(src){
  var r = new Reform(src);
  return (val)=>r.reform(val);
};

module.exports = {
  Reform,
  makeReform
};
