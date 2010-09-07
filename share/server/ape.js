var dbname = null;
var ddoc = null;
var requests = {};

var init = function(info) {
  dbname = info.dbname;
  ddoc = info.ddoc;
  return true;
};

var log = function(mesg) {
  if(typeof mesg !== "string") {
    mesg = JSON.stringify(mesg);
  }
  erlang.send(mesg);
};

var init_req = function(reqid, req) {
  ctx = requests[reqid] = {};
  ctx.requests = [];
  ctx.processing = {};
  ctx.sandbox = erlang.evalcx("");

  ctx.sandbox.log = log;

  ctx.sandbox.open_doc = function(docid, options) {
    ctx.requests.push(["open_doc", [docid], options]);
  };
    
  ctx.sandbox.save_doc = function(doc, options) {
    ctx.requests.push(["save_doc", [doc], options]);
  };

  ctx.sandbox.delete_doc = function(doc, options) {
    ctx.requests.push(["delete_doc", [doc], options]);
  };

  ctx.sandbox.query_view = function(view_id, options) {
    ctx.requests.push(["query_view", [view_id], options]);
  };

  ctx.sandbox.respond = function(resp) {
    ctx.requests.push(["response", resp]);
  };

  ctx.sandbox.require = function(name, parent) {
    parent = parent || {};
    var resolved = resolve(name.split("/"), parent.actual, ddoc, parent.id);
    var src = "function(module, exports, require) {" + resolved[0] + "}";
    var func = erlang.evalcx(src, ctx.sandbox);
    var module = {"id": resolved[2], "actual": resolved[1], "exports": []};

    try { 
      var reqfunc = function(name) {return require(name, module);};
      func.apply(null, [module, module.exports, reqfunc]);
    } catch(e) {
      var msg = "require('" + name + "') raised error " + e.toSource();
      throw(["error", "compilation_error", msg]);
    }

    return module.exports;
  };  

  eval(ddoc.app);
  ctx.app = erlang.evalcx(ddoc.app, ctx.sandbox);
  if(typeof ctx.app !== "function") {
    throw("Invalid function: " + ddoc.app);
  }
  ctx.app(req);
  return true;
};

var next_req = function(reqid) {
  var ctx = requests[reqid];
  if(ctx.requests.length < 1) {
    return null;
  }
  var req = ctx.current = ctx.requests.shift();
  return [req[0], req[1]];
};

var respond = function(reqid, resp) {
  var ctx = requests[reqid];
  ctx.current[2].callback(resp);
  ctx.current = null;
  return true;
};

var start_view = function(reqid, info) {
  var ctx = requests[reqid];
  ctx.current[2].startback(info);
  return true;
};

var send_row = function(reqid, row) {
  var ctx = requests[reqid];
  ctx.current[2].rowback(row);
  return true;
};

var end_view = function(reqid) {
  ctx.current[2].endback();
  return true;
};

var error = function(reqid, err) {
  var ctx = requests[reqid];
  ctx.current[2].errback(err);
  ctx.current = null;
  return true;
};

var resolve = function(names, parent, current, path) {
  if(names.length == 0) {
    if(typeof current != "string") {
      throw(["error", "bad_require_path", "Require paths must be a string."]);
    }
    return [current, parent, path];
  }

  var n = names.shift();
  if(n == '..') {
    if(!(parent && parent.parent)) {
      var obj = JSON.stringify(current);
      throw(["error", "bad_require_path", "Object has no parent: " + obj]);
    }
    path = path.slice(0, path.lastIndexOf("/"));
    return resolve(names, parent.parent.parent, parent.parent, path);
  } else if(n == ".") {
    if(!parent) {
      var obj = JSON.stringify(current);
      throw(["error", "bad_require_path", "Object has no parent: " + obj]);
    }
    return resolve(names, parent.parent, parent, path);
  }

  if(!current[n]) {
    throw(["error", "bad_require_path", "No such property " + n + ": " + obj]);
  }
  
  var p = current;
  current = current[n];
  current.parent = p;
  path = path ? path + '/' + n : n;
  return resolve(names, p, current, path);
};

