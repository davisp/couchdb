var map_funs = [];
var results = [];

var emit = function(key, value) {
  results.push([key, value]);
};

var map_doc = function(doc) {
  var ret = [];
  map_funs.forEach(function(func) {
    results = [];
    func(doc);
    ret.push(results);
  });
  return ret;
};
