function(req) {
  var util = require("util");

  var run_view = function(opts, callback, body) {
    var count = 0;
    body = body || "";
    query_view(null, {
      "options": opts,
      "startback": function(info) {
        body += JSON.stringify(info) + "\n";
      },
      "rowback": function(row) {
        body += JSON.stringify(row.key) + "\n";
        count += 1;
        if(count >= 5) throw("stop");
      },
      "endback": function() {
        body += "And done.";
        callback(body);
      }
    });
  };

  open_doc("_design/ape", {
    "callback": function(doc) {
      var newdoc = {"body": doc._id + ":" + doc._rev};
      save_doc(newdoc, {
        "callback": function(info) {
          log("Saved a document.");
          newdoc._id = info.id;
          newdoc._rev = info.rev;
          delete_doc(newdoc, {
            "callback": function() {}
          });
        }
      });
      save_doc({"other": "and some stuff"}, {
        "callback": function() {}
      });
      run_view(null, function(body1) {
        run_view({start_key: "2"}, function(body2) {
          run_view({end_key: "2"}, function(body3) {
            util.mkresponse(200, body3);
          }, body2);
        }, body1);
      });
    },
    "errback": function(err) {
      util.mkresponse(500, "Error: " + JSON.stringify(err) + "\n");
    }
  });
}
