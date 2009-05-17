// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy
// of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations under
// the License.

function compileFunction(source) {
  try {
    var functionObject = sandbox ? evalcx(source, sandbox) : eval(source);
  } catch (err) {
    throw {error: "compilation_error",
      reason: err.toString() + " (" + source + ")"};
  }
  if (typeof(functionObject) == "function") {
    return functionObject;
  } else {
    throw {error: "compilation_error",
      reason: "expression does not eval to a function. (" + source + ")"};
  }
}

function recursivelySeal(obj) {
  seal(obj);
  for (var propname in obj) {
    if (typeof doc[propname] == "object") {
      recursivelySeal(doc[propname]);
    }
  }
}

var responseSent;
// prints the object as JSON, and rescues and logs any toJSON() related errors
function respond(obj) {
  responseSent = true;
  try {
    print(JSON.stringify(obj));  
  } catch(e) {
    log("Error converting object to JSON: " + e.toString());
  }
};

log = function(message) {
  if (typeof message == "undefined") {
    message = "Error: attempting to log message of 'undefined'.";
  } else if (typeof message != "string") {
    message = JSON.stringify(message);
  }
  print(JSON.stringify({log: message}));
};
