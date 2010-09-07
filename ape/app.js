function(req) {
  var body = "Hello, world!\n";
  respond({
    "code": 200,
    "headers": {
      "Content-Type": "text/plain",
      "Content-Length": "" + body.length
    },
    "body": body
  });
}
