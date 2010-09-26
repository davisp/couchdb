#!/usr/bin/env escript
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-record(req, {method=get, path="", headers=[], body="", opts=[]}).

default_config() ->
    [
        test_util:build_file("etc/couchdb/default_dev.ini"),
        test_util:source_file("test/etap/180-http-proxy.ini")
    ].

server() -> "http://127.0.0.1:5985/_test/".

main(_) ->
    test_util:init_code_path(),

    etap:plan(unknown),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag("Test died abnormally: ~p", [Other]),
            etap:bail("Bad return value.")
    end,
    ok.

check_request(Name, Req, Assert, Expect) ->
    case Assert of
        no_assert ->
            ok;
        _ ->
            test_web:set_assert(Assert)
    end,
    Url = case proplists:lookup(url, Req#req.opts) of
        none ->
            server() ++ Req#req.path;
        {url, DestUrl} ->
            DestUrl
    end,
    Opts = [{headers_as_is, true} | Req#req.opts],
    %etap:diag("URL: ~p", [Url]),
    Resp =ibrowse:send_req(
        Url, Req#req.headers, Req#req.method, Req#req.body, Opts
    ),
    etap:fun_is(Expect, Resp, Name),
    case Assert of
        no_assert ->
            ok;
        _ ->
            etap:is(test_web:check_last(), was_ok, Name ++ " - request handled")
    end.

test() ->
    couch_server_sup:start_link(default_config()),
    ibrowse:start(),
    crypto:start(),
    test_web:start_link(),
    
    test_basic(),
    test_alternate_status(),
    test_passes_header(),
    test_passes_host_header(),
    test_passes_header_back(),
    test_uses_same_version(),
    test_passes_body(),

    test_connect_error(),
    
    ok.

test_basic() ->
    Assert = fun(Req) ->
        'GET' = Req:get(method),
        "/_test/" = Req:get(path),
        undefined = Req:get(body_length),
        undefined = Req:recv_body(),
        {ok, {200, [{"Content-Type", "text/plain"}], "ok"}}
    end,
    Expect = fun({ok, "200", _, "ok"}) -> true; (_) -> false end,
    check_request("Basic proxy test", #req{}, Assert, Expect).

test_alternate_status() ->
    Assert = fun(Req) ->
        "/_test/alternate_status" = Req:get(path),
        {ok, {201, [], "ok"}}
    end,
    Expect = fun({ok, "201", _, "ok"}) -> true; (_) -> false end,
    Req = #req{path="alternate_status"},
    check_request("Alternate status", Req, Assert, Expect).

test_passes_header() ->
    Assert = fun(Req) ->
        "/_test/passes_header" = Req:get(path),
        "plankton" = Req:get_header_value("X-CouchDB-Ralph"),
        {ok, {200, [], "ok"}}
    end,
    Expect = fun({ok, "200", _, "ok"}) -> true; (_) -> false end,
    Req = #req{
        path="passes_header",
        headers=[{"X-CouchDB-Ralph", "plankton"}]
    },
    check_request("Passes header", Req, Assert, Expect).

test_passes_host_header() ->
    Assert = fun(Req) ->
        "/_test/passes_host_header" = Req:get(path),
        "www.google.com" = Req:get_header_value("Host"),
        {ok, {200, [], "ok"}}
    end,
    Expect = fun({ok, "200", _, "ok"}) -> true; (_) -> false end,
    Req = #req{
        path="passes_host_header",
        headers=[{"Host", "www.google.com"}]
    },
    check_request("Passes host header", Req, Assert, Expect).

test_passes_header_back() ->
    Assert = fun(Req) ->
        "/_test/passes_header_back" = Req:get(path),
        {ok, {200, [{"X-CouchDB-Plankton", "ralph"}], "ok"}}
    end,
    Expect = fun
        ({ok, "200", Headers, "ok"}) ->
            lists:member({"X-CouchDB-Plankton", "ralph"}, Headers);
        (_) ->
            false
    end,
    Req = #req{path="passes_header_back"},
    check_request("Passes header back", Req, Assert, Expect).

test_uses_same_version() ->
    Assert = fun(Req) ->
        "/_test/uses_same_version" = Req:get(path),
        {1, 0} = Req:get(version),
        {ok, {200, [], "ok"}}
    end,
    Expect = fun({ok, "200", _, "ok"}) -> true; (_) -> false end,
    Req = #req{
        path="uses_same_version",
        opts=[{http_vsn, {1, 0}}]
    },
    check_request("Uses same version", Req, Assert, Expect).

test_passes_body() ->
    Assert = fun(Req) ->
        'PUT' = Req:get(method),
        "/_test/passes_body" = Req:get(path),
        <<"Hooray!">> = Req:recv_body(),
        {ok, {201, [], "ok"}}
    end,
    Expect = fun({ok, "201", _, "ok"}) -> true; (_) -> false end,
    Req = #req{
        method=put,
        path="passes_body",
        body="Hooray!"
    },
    check_request("Passes body", Req, Assert, Expect).

test_connect_error() ->
    Expect = fun
        ({ok, "500", Headers, Body}) ->
            {Json} = couch_util:json_decode(Body),
            etap:is(
                proplists:get_value(<<"reason">>, Json, undefined),
                <<"{conn_failed,{error,econnrefused}}">>,
                "Error reason is conn refused."
            ),
            true;
        (_) ->
            false
    end,
    Req = #req{opts=[{url, "http://127.0.0.1:5984/_error"}]},
    check_request("Connect error", Req, no_assert, Expect).
