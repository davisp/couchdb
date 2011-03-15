#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./src/couchdb -pa ./src/mochiweb -sasl errlog_type false -noshell

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

%% TODO: Figure out how to -include("couch_db.hrl")

-record(doc, {
    id = <<"">>,
    revs = {0, []},
    body = <<"{}">>,
    atts = [],
    deleted = false,
    meta = []
}).

-record(att, {
    name,
    type,
    att_len,
    disk_len,
    md5 = <<>>,
    revpos = 0,
    data,
    encoding = identity
}).

-define(json(EJson), iolist_to_binary(couch_util:json_encode(EJson))).

default_config() ->
    test_util:build_file("etc/couchdb/default_dev.ini").

main(_) ->
    test_util:init_code_path(),
    etap:plan(16),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail()
    end,
    ok.

test() ->
    couch_config:start_link([default_config()]),
    couch_config:set("attachments", "compression_level", "0"),
    ok = test_to_json_success(),
    ok.

test_to_json_success() ->
    Cases = [
        {
            #doc{},
            ?json({[{<<"_id">>, <<"">>}]}),
            "Empty docs are {\"_id\": \"\"}"
        },
        {
            #doc{id = <<"foo">>},
            ?json({[{<<"_id">>, <<"foo">>}]}),
            "_id is added."
        },
        {
            #doc{revs = {5, ["foo"]}},
            ?json({[{<<"_id">>, <<>>}, {<<"_rev">>, <<"5-foo">>}]}),
            "_rev is added."
        },
        {
            [revs],
            #doc{revs = {5, [<<"first">>, <<"second">>]}},
            ?json({[
                {<<"_id">>, <<>>},
                {<<"_rev">>, <<"5-first">>},
                {<<"_revisions">>, {[
                    {<<"start">>, 5},
                    {<<"ids">>, [<<"first">>, <<"second">>]}
                ]}}
            ]}),
            "_revisions include with revs option"
        },
        {
            #doc{body = <<"{\"foo\":\"bar\"}">>},
            ?json({[{<<"_id">>, <<>>}, {<<"foo">>, <<"bar">>}]}),
            "Arbitrary fields are added."
        },
        {
            % body can also be EJSON
            #doc{body = {[{<<"foo">>, <<"bar">>}]}},
            ?json({[{<<"_id">>, <<>>}, {<<"foo">>, <<"bar">>}]}),
            "Arbitrary fields are added."
        },
        {
            #doc{deleted = true, body = <<"{\"foo\":\"bar\"}">>},
            ?json({[{<<"_id">>, <<>>}, {<<"_deleted">>, true}, {<<"foo">>, <<"bar">>}]}),
            "Deleted docs no longer drop body members."
        },
        {
            % body can also be EJSON
            #doc{deleted = true, body = {[{<<"foo">>, <<"bar">>}]}},
            ?json({[{<<"_id">>, <<>>}, {<<"_deleted">>, true}, {<<"foo">>, <<"bar">>}]}),
            "Deleted docs no longer drop body members."
        },
        {
            #doc{meta = [
                {revs_info, 4, [{<<"fin">>, deleted}, {<<"zim">>, missing}]}
            ]},
            ?json({[
                {<<"_id">>, <<>>},
                {<<"_revs_info">>, [
                    {[{<<"rev">>, <<"4-fin">>}, {<<"status">>, <<"deleted">>}]},
                    {[{<<"rev">>, <<"3-zim">>}, {<<"status">>, <<"missing">>}]}
                ]}
            ]}),
            "_revs_info field is added correctly."
        },
        {
            #doc{
                id = <<"foobar">>,
                body = <<"{\"value\":1,\"data\":\"qwerty\"}">>,
                meta = [
                    {revs_info, 4, [{<<"fin">>, deleted}, {<<"zim">>, missing}]}
                ]
            },
            ?json({[
                {<<"_id">>, <<"foobar">>},
                {<<"_revs_info">>, [
                    {[{<<"rev">>, <<"4-fin">>}, {<<"status">>, <<"deleted">>}]},
                    {[{<<"rev">>, <<"3-zim">>}, {<<"status">>, <<"missing">>}]}
                ]},
                {<<"value">>, 1},
                {<<"data">>, <<"qwerty">>}
            ]}),
            "_revs_info field is added correctly."
        },
        {
            #doc{meta = [{local_seq, 5}]},
            ?json({[{<<"_id">>, <<>>}, {<<"_local_seq">>, 5}]}),
            "_local_seq is added as an integer."
        },
        {
            #doc{meta = [{conflicts, [{3, <<"yep">>}, {1, <<"snow">>}]}]},
            ?json({[
                {<<"_id">>, <<>>},
                {<<"_conflicts">>, [<<"3-yep">>, <<"1-snow">>]}
            ]}),
            "_conflicts is added as an array of strings."
        },
        {
            #doc{meta = [{deleted_conflicts, [{10923, <<"big_cowboy_hat">>}]}]},
            ?json({[
                {<<"_id">>, <<>>},
                {<<"_deleted_conflicts">>, [<<"10923-big_cowboy_hat">>]}
            ]}),
            "_deleted_conflicts is added as an array of strings."
        },
        {
            #doc{atts = [
                #att{
                    name = <<"big.xml">>,
                    type = <<"xml/sucks">>,
                    data = fun() -> ok end,
                    revpos = 1,
                    att_len = 400,
                    disk_len = 400
                },
                #att{
                    name = <<"fast.json">>,
                    type = <<"json/ftw">>,
                    data = <<"{\"so\": \"there!\"}">>,
                    revpos = 1,
                    att_len = 16,
                    disk_len = 16
                }
            ]},
            ?json({[
                {<<"_id">>, <<>>},
                {<<"_attachments">>, {[
                    {<<"big.xml">>, {[
                        {<<"content_type">>, <<"xml/sucks">>},
                        {<<"revpos">>, 1},
                        {<<"length">>, 400},
                        {<<"stub">>, true}
                    ]}},
                    {<<"fast.json">>, {[
                        {<<"content_type">>, <<"json/ftw">>},
                        {<<"revpos">>, 1},
                        {<<"length">>, 16},
                        {<<"stub">>, true}
                    ]}}
                ]}}
            ]}),
            "Attachments attached as stubs only include a length."
        },
        {
            [attachments],
            #doc{atts = [
                #att{
                    name = <<"stuff.txt">>,
                    type = <<"text/plain">>,
                    data = fun() -> <<"diet pepsi">> end,
                    revpos = 1,
                    att_len = 10,
                    disk_len = 10
                },
                #att{
                    name = <<"food.now">>,
                    type = <<"application/food">>,
                    revpos = 1,
                    data = <<"sammich">>
                }
            ]},
            ?json({[
                {<<"_id">>, <<>>},
                {<<"_attachments">>, {[
                    {<<"stuff.txt">>, {[
                        {<<"content_type">>, <<"text/plain">>},
                        {<<"revpos">>, 1},
                        {<<"data">>, <<"ZGlldCBwZXBzaQ==">>}
                    ]}},
                    {<<"food.now">>, {[
                        {<<"content_type">>, <<"application/food">>},
                        {<<"revpos">>, 1},
                        {<<"data">>, <<"c2FtbWljaA==">>}
                    ]}}
                ]}}
            ]}),
            "Attachments included inline with attachments option."
        },
        {
            [attachments, follows],
            #doc{atts = [
                #att{
                    name = <<"stuff.txt">>,
                    type = <<"text/plain">>,
                    data = fun() -> <<"diet pepsi">> end,
                    revpos = 1,
                    att_len = 10,
                    disk_len = 10
                },
                #att{
                    name = <<"food.now">>,
                    type = <<"application/food">>,
                    revpos = 1,
                    data = <<"sammich">>,
                    disk_len = 7,
                    att_len = 7
                }
            ]},
            ?json({[
                {<<"_id">>, <<>>},
                {<<"_attachments">>, {[
                    {<<"stuff.txt">>, {[
                        {<<"content_type">>, <<"text/plain">>},
                        {<<"revpos">>, 1},
                        {<<"length">>, 10},
                        {<<"follows">>, true}
                    ]}},
                    {<<"food.now">>, {[
                        {<<"content_type">>, <<"application/food">>},
                        {<<"revpos">>, 1},
                        {<<"length">>, 7},
                        {<<"follows">>, true}
                    ]}}
                ]}}
            ]}),
            "Attachments with follows option."
        }
    ],

    lists:foreach(fun
        ({Doc, RawJson, Mesg}) ->
            etap:is(couch_doc:doc_to_json(Doc, []), RawJson, Mesg);
        ({Options, Doc, RawJson, Mesg}) ->
            etap:is(couch_doc:doc_to_json(Doc, Options), RawJson, Mesg)
    end, Cases),
    ok.

