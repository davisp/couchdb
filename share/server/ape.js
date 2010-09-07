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
    ctx.requests.push(["open_doc", docid, options]);
  };
    
  ctx.sandbox.save_doc = function(doc, options) {
    ctx.requests.push(["save_doc", docid, options]);
  };

  ctx.sandbox.delete_doc = function(docid, options) {
    ctx.requests.push(["delete_doc", docid, options]);
  };

  ctx.sandbox.respond = function(resp) {
    ctx.requests.push(["response", resp]);
  };

  ctx.app = erlang.evalcx(ddoc.app, ctx.sandbox);
  if(typeof ctx.app !== "function") {
    throw("Invalid function: " + ddoc.app);
  }
  ctx.app(req);
  return true;
};

var next_req = function(reqid) {
  var ctx = requests[reqid];
  req = ctx.current = ctx.requests.shift();
  return [req[0], req[1]];
};

var respond = function(reqid, resp) {
  var ctx = requests[reqid];
  ctx.current.callback(resp);
  ctx.current = null;
  return true;
};

var error = function(reqid, err) {
  var ctx = requests[reqid];
  ctx.current.errback(err);
  ctx.current = null;
  return true;
};

