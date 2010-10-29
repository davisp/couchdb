
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

couchTests.app_engine = function(debug) {
  var db = new CouchDB("test_suite_db", {"X-Couch-Full-Commit":"false"});

  var setUp = function(app_fun, opts) {
    opts = opts || {};
    db.deleteDb();
    db.createDb();
    var ddoc = {
      "_id": "_design/app_engine",
      "app": stringFun(app_fun),
      "utils": "exports.mkresponse = function(code, body) {\n" +
        "respond({\n" +
        "\"code\": code,\n" +
        "\"headers\": {\n" +
        "\"Content-Type\": \"text/plain\",\n" +
        "\"Content-Length\": \"\" + body.length\n" +
        "},\n" +
        "\"body\": body\n" +
        "});\n" +
        "};"
    };
    if(opts.map_fun) {
      ddoc.views = {"test": {"map": stringFun(opts.map_fun)}};
      if(opts.red_fun) {
        ddoc.views.test.reduce = stringFun(opts.red_fun);
      }
    }
    if(opts.num_docs) {
      db.bulkSave(makeDocs(1, opts.num_docs + 1));
    }
    db.save(ddoc);
    return ddoc;
  };

  var mkReq = function(verb, path, parse) {
    path = "/test_suite_db/_design/app_engine/_app" + path;
    var resp = CouchDB.request(verb, path);
    if(parse) {
      return JSON.parse(resp.responseText);
    } else {
      return resp.responseText;
    }
  };

  // Open doc test
  var ddoc = setUp(function(req) {
    utils = require("utils");
    open_doc("_design/app_engine", {
      "callback": function(doc) {utils.mkresponse(200, doc._rev);},
      "errback": function(doc) {utils.mkresponse(500, "error opening doc.");}
    });
  });
  var resp = mkReq("GET", "/");
  T(resp == ddoc._rev);

  // Save doc tests.
  setUp(function(req) {
    utils = require("utils");
    if(req.method != "POST") {
      utils.mkresponse(500, "invalid method");
      return;
    }
    var docid = req.path[4];
    save_doc({"_id": docid, "app": "engine"}, {
      "callback": function(doc) {utils.mkresponse(200, "saved");},
      "errback": function(err) {utils.mkresponse(500, err.error);}
    });
  });
  // Can return an error.
  var resp = mkReq("GET", "/foo");
  T(resp == "invalid method");
  // Can save a doc.
  var resp = mkReq("POST", "/foo");
  T(resp == "saved");
  // Can return an error from saving.
  var resp = mkReq("POST", "/foo");
  T(resp == "conflict");

  // Delete doc tests
  setUp(function(req) {
    utils = require("utils");
    if(req.method != "DELETE") {
      utils.mkresponse(500, "invalid method");
      return;
    }
    var docid = req.path[4];
    open_doc(docid, {
      "callback": function(doc) {
        if(doc._id == "2") delete doc._rev;
        delete_doc(doc, {
          "callback": function(doc) {utils.mkresponse(200, "deleted");},
          "errback": function(err) {utils.mkresponse(500, err.error);}
        });
      },
      "errback": function(err) {utils.mkresponse(500, "error opening doc");}
    });
  }, {"num_docs": 2});
  // Checks the method
  var resp = mkReq("GET", "/foo");
  T(resp == "invalid method");
  // Can delete a doc
  var resp = mkReq("DELETE", "/1");
  T(resp == "deleted");
  // Can return an error.
  var resp = mkReq("DELETE", "/2");
  T(resp == "conflict");

  // _all_docs view
  setUp(function(req) {
    utils = require("utils");
    var body = "";
    query_view(null, {
      "options": {},
      "startback": function(info) {
        body += "<";
      },
      "rowback": function(row) {
        if(row.key.length == 1) {
          body += row.key;
        }
      },
      "endback": function() {
        body += ">";
        utils.mkresponse(200, body);
      }
    });
  }, {"num_docs": 5});
  // Check our _all_docs
  var resp = mkReq("GET", "/");
  T(resp == "<12345>");

  // _all_docs with start and end key
  setUp(function(req) {
    utils = require("utils");
    var body = "";
    query_view(null, {
      "options": {
        "start_key": "2",
        "end_key": "4"
      },
      "startback": function(info) {
        body += "<";
      },
      "rowback": function(row) {
        if(row.key.length == 1) {
          body += row.key;
        }
      },
      "endback": function() {
        body += ">";
        utils.mkresponse(200, body);
      }
    });
  }, {"num_docs": 5});
  var resp = mkReq("GET", "/");
  T(resp == "<234>");

  setUp(function(req) {
    utils = require("utils");
    var body = "";
    query_view("test", {
      "options": {},
      "startback": function(info) {
        body += "<";
      },
      "rowback": function(row) {
        body += row.key;
      },
      "endback": function() {
        body += ">";
        utils.mkresponse(200, body);
      }
    });
  }, {
    "num_docs": 5,
    "map_fun": function(doc) {
      if(!doc.integer) return;
      emit(doc._id, null);
    }
  });
  var resp = mkReq("GET", "/");
  T(resp == "<12345>");

  setUp(function(req) {
    utils = require("utils");
    var body = "";
    query_view("test", {
      "options": {
        "start_key": "2",
        "end_key": "4"
      },
      "startback": function(info) {
        body += "<";
      },
      "rowback": function(row) {
        body += row.key;
      },
      "endback": function() {
        body += ">";
        utils.mkresponse(200, body);
      }
    });
  }, {
    "num_docs": 5,
    "map_fun": function(doc) {
      if(!doc.integer) return;
      emit(doc._id, null);
    }
  });
  var resp = mkReq("GET", "/");
  T(resp == "<234>");


};
