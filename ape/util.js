exports.mkresponse = function(code, body) {
  respond({
    "code": code,
    "headers": {
      "Content-Type": "text/plain",
      "Content-Length": "" + body.length
    },
    "body": body
  });
};


