function(req) {
  var util = require("util");
  for(var i in util) {
    log("I: " + i);
  }

  open_doc("_design/ape", {
    "callback": function(doc) {
      var newdoc = {"body": doc._id + ":" + doc._rev};
      save_doc(newdoc, {callback: function(info) {
        log("Saved a document.");
        newdoc._id = info.id;
        newdoc._rev = info.rev;
        delete_doc(newdoc, {callback: function(info) {
          var body = "Created and deleted: " + info.id + ":" + info.rev;
          util.mkresponse(200, body);
        }});
      }});
    },
    "errback": function(err) {
      util.mkresponse(500, "Error: " + JSON.stringify(err) + "\n");
    }
  });
}
