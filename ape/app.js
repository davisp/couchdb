function(req) {
  var mkresponse = function(code, body) {
    respond({
      "code": code,
      "headers": {
        "Content-Type": "text/plain",
        "Content-Length": "" + body.length
      },
      "body": body
    });
  };

  open_doc("_design/ape", {
    "callback": function(doc) {
      var newdoc = {"body": doc._id + ":" + doc._rev};
      save_doc(newdoc, {callback: function(info) {
        log("Saved a document.");
        newdoc._id = info.id;
        newdoc._rev = info.rev;
        delete_doc(newdoc, {callback: function(info) {
          mkresponse(200, "Created and deleted: " + info.id + ":" + info.rev);
        }});
      }});
    },
    "errback": function(err) {
      mkresponse(500, "Error: " + JSON.stringify(err) + "\n");
    }
  });
}
