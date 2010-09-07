function(req) {
  var util = require("util");

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
      var body = "";
      query_view(null, {
        "startback": function(info) {
          body += JSON.stringify(info) + "\n";
        },
        "rowback": function(row) {
          body += JSON.stringify(row) + "\n";
        },
        "endback": function() {
          body += "And done.";
          util.mkresponse(200, body);
        }
      });
    },
    "errback": function(err) {
      util.mkresponse(500, "Error: " + JSON.stringify(err) + "\n");
    }
  });
}
