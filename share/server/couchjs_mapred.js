// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

/*
    http://www.JSON.org/json2.js
    2010-03-20
    Public Domain.
    NO WARRANTY EXPRESSED OR IMPLIED. USE AT YOUR OWN RISK.
    See http://www.JSON.org/js.html
*/

if (!this.JSON) {
    this.JSON = {};
}

(function () {
    function f(n) {
        return n < 10 ? '0' + n : n;
    }
    if (typeof Date.prototype.toJSON !== 'function') {
        Date.prototype.toJSON = function (key) {
            return isFinite(this.valueOf()) ?
                   this.getUTCFullYear()   + '-' +
                 f(this.getUTCMonth() + 1) + '-' +
                 f(this.getUTCDate())      + 'T' +
                 f(this.getUTCHours())     + ':' +
                 f(this.getUTCMinutes())   + ':' +
                 f(this.getUTCSeconds())   + 'Z' : null;
        };
        String.prototype.toJSON =
        Number.prototype.toJSON =
        Boolean.prototype.toJSON = function (key) {
            return this.valueOf();
        };
    }
    var cx = /[\u0000\u00ad\u0600-\u0604\u070f\u17b4\u17b5\u200c-\u200f\u2028-\u202f\u2060-\u206f\ufeff\ufff0-\uffff]/g,
        escapable = /[\\\"\x00-\x1f\x7f-\x9f\u00ad\u0600-\u0604\u070f\u17b4\u17b5\u200c-\u200f\u2028-\u202f\u2060-\u206f\ufeff\ufff0-\uffff]/g,
        gap,
        indent,
        meta = {
            '\b': '\\b',
            '\t': '\\t',
            '\n': '\\n',
            '\f': '\\f',
            '\r': '\\r',
            '"' : '\\"',
            '\\': '\\\\'
        },
        rep;
    function quote(string) {
        escapable.lastIndex = 0;
        return escapable.test(string) ?
            '"' + string.replace(escapable, function (a) {
                var c = meta[a];
                return typeof c === 'string' ? c :
                    '\\u' + ('0000' + a.charCodeAt(0).toString(16)).slice(-4);
            }) + '"' :
            '"' + string + '"';
    }
    function str(key, holder) {
        var i,          // The loop counter.
            k,          // The member key.
            v,          // The member value.
            length,
            mind = gap,
            partial,
            value = holder[key];
        if (value && typeof value === 'object' &&
                typeof value.toJSON === 'function') {
            value = value.toJSON(key);
        }
        if (typeof rep === 'function') {
            value = rep.call(holder, key, value);
        }
        switch (typeof value) {
        case 'string':
            return quote(value);
        case 'number':
            return isFinite(value) ? String(value) : 'null';
        case 'boolean':
        case 'null':
            return String(value);
        case 'object':
            if (!value) {
                return 'null';
            }
            gap += indent;
            partial = [];
            if (Object.prototype.toString.apply(value) === '[object Array]') {
                length = value.length;
                for (i = 0; i < length; i += 1) {
                    partial[i] = str(i, value) || 'null';
                }
                v = partial.length === 0 ? '[]' :
                    gap ? '[\n' + gap +
                            partial.join(',\n' + gap) + '\n' +
                                mind + ']' :
                          '[' + partial.join(',') + ']';
                gap = mind;
                return v;
            }
            if (rep && typeof rep === 'object') {
                length = rep.length;
                for (i = 0; i < length; i += 1) {
                    k = rep[i];
                    if (typeof k === 'string') {
                        v = str(k, value);
                        if (v) {
                            partial.push(quote(k) + (gap ? ': ' : ':') + v);
                        }
                    }
                }
            } else {
                for (k in value) {
                    if (Object.hasOwnProperty.call(value, k)) {
                        v = str(k, value);
                        if (v) {
                            partial.push(quote(k) + (gap ? ': ' : ':') + v);
                        }
                    }
                }
            }
            v = partial.length === 0 ? '{}' :
                gap ? '{\n' + gap + partial.join(',\n' + gap) + '\n' +
                        mind + '}' : '{' + partial.join(',') + '}';
            gap = mind;
            return v;
        }
    }
    if (typeof JSON.stringify !== 'function') {
        JSON.stringify = function (value, replacer, space) {
            var i;
            gap = '';
            indent = '';
            if (typeof space === 'number') {
                for (i = 0; i < space; i += 1) {
                    indent += ' ';
                }
            } else if (typeof space === 'string') {
                indent = space;
            }
            rep = replacer;
            if (replacer && typeof replacer !== 'function' &&
                    (typeof replacer !== 'object' ||
                     typeof replacer.length !== 'number')) {
                throw new Error('JSON.stringify');
            }
            return str('', {'': value});
        };
    }
    if (typeof JSON.parse !== 'function') {
        JSON.parse = function (text, reviver) {
            var j;
            function walk(holder, key) {
                var k, v, value = holder[key];
                if (value && typeof value === 'object') {
                    for (k in value) {
                        if (Object.hasOwnProperty.call(value, k)) {
                            v = walk(value, k);
                            if (v !== undefined) {
                                value[k] = v;
                            } else {
                                delete value[k];
                            }
                        }
                    }
                }
                return reviver.call(holder, key, value);
            }
            text = String(text);
            cx.lastIndex = 0;
            if (cx.test(text)) {
                text = text.replace(cx, function (a) {
                    return '\\u' +
                        ('0000' + a.charCodeAt(0).toString(16)).slice(-4);
                });
            }
            if (/^[\],:{}\s]*$/.
test(text.replace(/\\(?:["\\\/bfnrt]|u[0-9a-fA-F]{4})/g, '@').
replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g, ']').
replace(/(?:^|:|,)(?:\s*\[)+/g, ''))) {
                j = eval('(' + text + ')');
                return typeof reviver === 'function' ?
                    walk({'': j}, '') : j;
            }
            throw new SyntaxError('JSON.parse');
        };
    }
}());

// My stuff

var server_config = {};
var map_funs = [];
var red_funs = [];
var map_results = [];
var line_length = null;
var sandbox = null;

var handle_error = function(e) {
  var type = e[0];
  if (type == "fatal") {
    e[0] = "error"; // we tell the client it was a fatal error by dying
    respond(e);
    quit(-1);
  } else if (type == "error") {
    respond(e);
  } else if (e.error && e.reason) {
    // compatibility with old error format
    respond(["error", e.error, e.reason]);
  } else {
    respond(["error","unnamed_error",e.toSource()]);
  }
};

var handle_view_error = function(err, doc) {
  if (err == "fatal_error") {
    // Only if it's a "fatal_error" do we exit. What's a fatal error?
    // That's for the query to decide.
    //
    // This will make it possible for queries to completely error out,
    // by catching their own local exception and rethrowing a
    // fatal_error. But by default if they don't do error handling we
    // just eat the exception and carry on.
    //
    // In this case we abort map processing but don't destroy the 
    // JavaScript process. If you need to destroy the JavaScript 
    // process, throw the error form matched by the block below.
    throw(["error", "map_runtime_error", "function raised 'fatal_error'"]);
  } else if (err[0] == "fatal") {
    // Throwing errors of the form ["fatal","error_key","reason"]
    // will kill the OS process. This is not normally what you want.
    throw(err);
  }
  var message = "function raised exception " + err.toSource();
  if (doc) message += " with doc._id " + doc._id;
  log(message);
};

var emit = function(key, value) {
    map_results[map_results.length-1].push([key, value]);
};

var sum = function(values) {
  var rv = 0;
  for(var i in values) {
    rv += values[i];
  }
  return rv;
};

var respond = function(obj) {
  try {
    print(JSON.stringify(obj));
  } catch(e) {
    print(JSON.stringify(["log", "Error converting object to JSON: " + e.toString()]));
    print(JSON.stringify(["log", "Error on object: " + obj.toSource()]));
  }
};

var log = function(message) {
  if(typeof(message) != "string") {
    message = JSON.stringify(message);
  }
  respond(["log", message]);
};

var compile_function = function(source) {    
  if (!source) throw(["error","not_found","missing function"]);
  var func = undefined;
  try {
    if (sandbox) {
      var func = evalcx(source, sandbox);
    } else {
      var func = eval(source);
    }
  } catch (err) {
    throw(["error", "compilation_error", err.toSource() + " (" + source + ")"]);
  }
  if(typeof(func) == "function") {
    return func;
  } else {
    throw(["error","compilation_error",
      "Expression does not eval to a function. (" + source.toSource() + ")"]);
  }
};

var init_sandbox = function() {
  try {
    // if possible, use evalcx (not always available)
    sandbox = evalcx('');
    sandbox.emit = emit;
    sandbox.sum = sum;
    sandbox.log = log;
    sandbox.toJSON = JSON.stringify;
    sandbox.JSON = JSON;
  } catch (e) {
    log(e.toSource());
  }
};

var configure = function(config) {
  server_config = config || {};
  respond(true);
}

var reset = function() {
  map_funs = new Array();
  red_funs = new Array();
  map_results = new Array();
  sandbox = null;
  init_sandbox();
  respond(true);
};

var compile = function(maps, reds) {
  try {
    map_funs = new Array(maps.length);
    for(var i = 0; i < maps.length; i++) {
      map_funs[i] = compile_function(maps[i]);
    }
    red_funs = new Array(reds.length);
    for(var i = 0; i < reds.length; i++) {
      red_funs[i] = compile_function(reds[i]);
    }
    respond(true);
  } catch(e) {
    handle_error(e);
  }
};

var map = function(doc) {
  map_results = new Array();
  map_funs.forEach(function(fun) {
    map_results.push([]);
    try {
      fun(doc);
    } catch(e) {
      handle_view_error(e);
      map_results[map_results.length-1] = [];
    }
  });
  respond([true, map_results]);
};

var check_reductions = function(reductions) {
  var reduce_line = JSON.stringify(reductions);
  var reduce_length = reduce_line.length;
  if (server_config && server_config.reduce_limit &&
          reduce_length > 200 && ((reduce_length * 2) > line_length)) {
    var reduce_preview = "Current output: '" + reduce_line.substring(0, 100) +
                        "'... (first 100 of " + reduce_length + " bytes)";
    throw(["error", "reduce_overflow_error",
            "Reduce output must shrink more rapidly: " + reduce_preview]);
  } else {
    print("[true," + reduce_line + "]");
  }
}

var reduce = function(kvs) {
  var keys = new Array(kvs.length);
  var vals = new Array(kvs.length);
  for(var i = 0; i < kvs.length; i++) {
      keys[i] = kvs[i][0];
      vals[i] = kvs[i][1];
  }
  var reductions = new Array(red_funs.length);
  for(var i = 0; i < red_funs.length; i++) {
      reductions[i] = red_funs[i](keys, vals, false);
  }
  if(keys.length > 1) {
    check_reductions(reductions);
  } else {
    respond([true, reductions]);
  }
}

var rereduce = function(vals) {
  var reductions = new Array(red_funs.length);
  for(var i = 0; i < red_funs.length; i++) {
    reductions[i] = red_funs[i](null, vals[i], true);
  }
  if(vals.length > 1) {
    check_reductions(reductions);
  } else {
    respond([true, reductions]);
  }
}

var commands = {
  "configure": configure,
  "reset": reset,
  "compile": compile,
  "map": map,
  "reduce": reduce,
  "rereduce": rereduce
};

init_sandbox();

while(line = readline()) {
  cmd = eval('('+line+')');
  line_length = line.length;
  try {
    cmdkey = cmd.shift();
    if(commands[cmdkey]) {
      commands[cmdkey].apply(null, cmd);
    } else {
      throw(["fatal", "unknown_command", "unknown command '" + cmdkey + "'"]);
    }
  } catch(e) {
    handle_error(e);
  }
}
