
import httplib
import urllib

try:
    import simplejson as json
except ImportError:
    import json


import erlang
a = erlang.Atom


DEFAULT_VM = erlang.VM(stdout=open("couchdb.log", "wb"))


class ServerError(Exception):
    def __init__(self, resp):
        self.resp = resp
    def __str__(self):
        print self.resp.body


def encodeuri(obj):
    if isinstance(obj, str):
        return urllib.quote(obj, safe="")
    qs = {}
    for k, v in obj.items():
        if not isinstance(v, basestring):
            v = json.dumps(v)
        qs[k] = v
    return urllib.urlencode(qs, doseq=1)


class Headers(dict):
    def __init__(self, *args, **kwargs):
        if len(args) > 1:
            raise ValueError("Too many positional arguments")
        items = []
        if len(args) == 1:
            if hasattr(args[0], "iteritems"):
                items = args[0].iteritems()
            elif hasattr(args[0], "items"):
                items = args[0].items()
            else:
                items = iter(args[0])
        else:
            items = iter(kwargs)
        for k, v in items:
            self.__setitem__(k, v)

    def __getitem__(self, key):
        return super(Headers, self).__getitem__(Headers.__norm__(key))

    def __setitem__(self, key, val):
        return super(Headers, self).__setitem__(Headers.__norm__(key), val)

    def __delitem__(self, key):
        return super(Headers, self).__delitem__(Headers.__norm__(key))

    def __contains__(self, key):
        return super(Headrs, self).__contains__(Headers.__norm__(key))

    @staticmethod
    def __norm__(key):
        if not isinstance(key, basestring):
            raise TypeError("Invalid header key: %s" % key)
        key = key.lower().split("-")
        return "-".join(map(lambda w: w.capitalize(), key))

    @staticmethod
    def fromkeys(S, v=None):
        S = map(Headers.__norm__, S)
        return dict.fromkeys(S, v)

    def get(self, key, val=None):
        return super(Headers, self).get(Headers.__norm__(key), val)

    has_key = __contains__

    def pop(self, key, val=None):
        return super(Headers, self).pop(Headers.__norm__(key), val)

    def setdefault(self, key, val=None):
        return super(Headeres, self).setdefault(Headers.__norm__(key), val)

    def update(self, E, **F):
        if hasattr(E, "keys"):
            for k in E:
                self[k] = E[k]
        else:
            for (k, v) in E:
                self[k] = v
        for k in F:
            self[k] = F[k]


class Server(object):
    def __init__(self, vm=None, headers=None):
        self._vm = vm or DEFAULT_VM
        self._conn = None
        self.headers = Headers(headers or {})
        self.resp = None

    def __getitem__(self, name):
        return Database(self, name)

    @property
    def vm(self):
        if not self._vm.booted:
            print "Booting VM"
            self._vm.boot()
            boot_script = """
            wait_for_start() ->
                wait_for_start(100).
            wait_for_start(0) ->
                error;
            wait_for_start(N) ->
                Servers = [
                    couch_server,
                    couch_config,
                    couch_httpd
                ],
                Pids = [whereis(S) || S <- Servers],
                case lists:any(fun(S) -> S == undefined end, Pids) of
                    true ->
                        timer:sleep(100),
                        wait_for_start(N-1);
                    false ->
                        ok
                end.
            idle() -> timer:sleep(10000), idle().
            main(_) ->
                spawn(fun() ->
                    test_util:init_code_path(),
                    couch_server_sup:start_link(test_util:config_files()),
                    idle()
                end),
                ok = wait_for_start().
            """
            self._vm.run(boot_script)
        return self._vm

    @property
    def conn(self):
        if not self.vm.booted:
            raise ValueError("Unable to boot vm.")
        if self._conn is None:
            self._conn = httplib.HTTPConnection(self.addr, self.port)
        return self._conn

    @property
    def addr(self):
        return self.vm.cx.couch_config.get("httpd", "bind_address", "127.0.01")

    @property
    def port(self):
        return self.vm.cx.mochiweb_socket_server.get(a.couch_httpd, a.port)

    def request(self, method, path, headers=None, body=None, check=True):
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        hdrs = self.headers.copy()
        if headers is not None:
            hdrs.update(headers)
        if "Content-Type" not in hdrs:
            hdrs["Content-Type"] = "application/json"
        if "Accept" not in hdrs:
            hdrs["Accept"] = "application/json"
        if not isinstance(body, str):
            body = json.dumps(body)
        self.conn.request(method, path, body, hdrs)
        self.resp = self.conn.getresponse()
        self.resp.body = self.resp.read()
        self.resp.json = None
        # Poor man's JSON detection
        if "json" in self.resp.getheader("Content-Type"):
            try:
                self.resp.json = json.loads(self.resp.body)
            except:
                pass
        if check:
            self.check_resp()
        return self.resp

    def check_resp(self):
        if self.resp.status >= 400:
            raise ServerError(self.resp)

    def get(self, *args, **kwargs):
        return self.request("GET", *args, **kwargs)

    def post(self, *args, **kwargs):
        return self.request("POSTS", *args, **kwargs)

    def put(self, *args, **kwargs):
        return self.request("PUT", *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self.request("DELETE", *args, **kwargs)

    def database(self, name, headers=None):
        return Database(self, name, headers=headers)

    def login(self, name, password):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "X-CouchDB-WWW-Authenticate": "Cookie"
        }
        body = uriencode({"name": name, "password": password})
        resp = self.post("/_session", headers=headers, body=body, check=False)
        return resp.json

    def logout(self):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "X-CouchDB-WWW-Authenticate": "Cookie"
        }
        return self.delete("/_session", headers=headers, check=False).json

    def session(self, **kwargs):
        return self.get("/_session", **kwargs).json

    def all_dbs(self):
        return self.get("/_all_dbs").json

    def version(self):
        return self.get("/").json["version"]

    def replicate(self, src, tgt, opts=None, headers=None, body=None):
        body = body or {}
        body["source"] = src
        body["target"] = tgt
        return self.post("/_replicate", headers=headers, body=body).json

    def get_stat(self, mod, key, test=False):
        qs = ""
        if test:
            qs = "?flush=true"
        path = "/_stats/%s/%s%s" % (mod, key, qs)
        return self.get(path, check=False).json


class Database(object):

    def __init__(self, server, name, headers=None):
        self.server = server
        self.name = name
        self.headers = headers or {}

    def uri(self, *args, **kwargs):
        path = "/" + "/".join(map(encodeuri, [self.name] + list(args)))
        if "qs" in kwargs:
            path += "?%s" % encodeuri(kwargs.pop("qs"))
        if len(kwargs) != 0:
            raise ValueError("Unkown kwargs: %s" % ', '.join(kwargs.keys()))
        return path

    def create_db(self):
        return self.server.put(self.uri()).json

    def delete_db(self):
        resp = self.server.delete(self.uri(), check=False)
        if resp.status == 404:
            return False
        self.server.check_resp()
        return resp.json

    def get_prop(self, prop):
        return self.server.get(self.uri(prop)).json

    def set_prop(self, prop, value):
        return self.server.put(self.uri(prop), body=json.dumps(value)).json

    def info(self):
        return self.server.get(self.uri()).json

    def design_info(self, docid):
        return self.server.get(self.uri(docid, "_info")).json

    def compact(self):
        return self.server.post(self.uri("_compact")).json

    def view_cleanup(self):
        return self.server.post(self.uri("_view_cleanup")).json

    def ensure_full_commit(self):
        return self.server.post(self.uri("_ensure_full_commit")).json

    def save(self, doc, headers=None, **kwargs):
        if "_id" not in doc:
            doc["_id"] = uuids.uuid4().hex
        uri = self.uri(doc["_id"], qs=kwargs)
        resp = self.server.put(uri, headers=headers, body=doc)
        doc["_rev"] = resp.json["rev"]
        return resp.json

    def open(self, docid, headers=None, **kwargs):
        uri = self.uri(docid, qs=kwargs)
        resp = self.server.get(uri, headers=headers, check=False)
        if resp.status == 404:
            return None
        self.server.check_resp()
        return resp.json

    def delete(self, doc):
        uri = self.uri(doc["_id"], qs={"rev": doc["_rev"]})
        resp = self.server.delete(uri)
        doc["_rev"] = resp.json["rev"]
        return resp.json

    def bulk_save(self, docs, **kwargs):
        for doc in docs:
            if "_id" not in doc:
                doc["_id"] = uuids.uuid4().hex
        body = {"docs": docs}
        body.update(kwargs)
        resp = this.server.post(self.uri("_bulk_docs"), body=body, check=False)
        if resp.status == 417:
            return {"errors": resp.json}
        self.server.check_resp()
        for i, doc in enumerate(docs):
            if resp.json[i] and resp.json[i]["rev"] and resp.json[i]["ok"]:
                doc["_rev"] = resp.json[i]["rev"]
        return resp.json

    def get_att(self, docid, filename):
        if not isinstance(docid, basestring):
            docid = docid["_id"]
        return self.server.get(self.uri(docid, filename)).body

    def save_att(self, doc, content, filename=None, ctype=None):
        if filename is None:
            if not hasattr(content, "name"):
                raise ValueError("No filename for attachment")
            filename = os.path.basename(content.name)
        if ctype is None:
            ctype = "application/octent-stream"
        hdrs = Headers({"Content-Type": ctype or "application/octet-stream"})
        uri = self.uri(doc["_id"], filename)
        resp = self.server.put(uri, headers=hdrs, body=content)
        doc["_rev"] = resp.json["rev"]
        return resp.json

    def delete_att(self, doc, filename):
        uri = self.uri(doc["_id"], filename, qs={"rev": doc["_rev"]})
        resp = self.server.delete(uri)
        doc["_rev"] = resp.json["rev"]
        return resp

    def changes(self, **kwargs):
        return self.server.get(self.uri("_changes", qs=kwargs)).json

    def all_docs(self, keys=None, **kwargs):
        uri = self.uri("_all_docs", qs=self.view_qs(kwargs))
        if keys is None:
            return self.server.get(uri).json
        return self.server.post(uri, body={"keys": keys}).json

    def design_docs(self):
        return self.all_docs(startkey="_design/", endkey="_design0")

    def query(self, map_fun, red_fun=None, lang=None, **kwargs):
        body = {
            "language": lang or "javascript",
            "map": map_fun,
        }
        if red_fun is not None:
            body["reduce"] = red_fun
        if kwargs.get("keys"):
            body["keys"] = kwargs.pop("keys")
        if kwargs.get("options"):
            body["options"] = kwargs.pop("options")
        uri = self.uri("_temp_view", qs=self.view_qs(kwargs))
        return self.server.post(uri, headers=headers, body=body).json

    def view(self, view_name, keys=None, **kwargs):
        parts = view_name.split("/", 1)
        qs = self.view_qs(kwargs)
        uri = self.uri("_design/%s" % parts[0], "_view", parts[1], qs=qs)
        if keys is None:
            resp = self.server.get(uri, check=False)
        else:
            resp = self.server.post(uri, body={"keys": keys}, check=False)
        if resp.status == 404:
            return None
        self.server.check_resp()
        return resp.json

    def view_qs(self, qs):
        ret = {}
        for k, v in qs.items():
            if k in ("key", "startkey", "endkey"):
                v = json.dumps(v)
            elif not isinstance(v, basestring):
                v = json.dumps(v)
            ret[k] = v
        return ret
