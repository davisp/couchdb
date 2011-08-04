#!/usr/bin/env escript
%% -*- erlang -*-

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

main(_) ->
    test_util:init_code_path(),

    etap:plan(unknown),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    timer:sleep(300),
    ok.

test() ->
    couch_server_sup:start_link(test_util:config_files()),

    {ok, Db} = couch_mrview_test_util:init_db(<<"foo">>),
    
    test_basic(Db),
    test_range(Db),
    test_rev_range(Db),

    ok.

test_basic(Db) ->
    Result = run_query(Db, []),
    Expect = {ok, [
        {total_and_offset, 10, 0},
        {row, [{key, 1}, {id, <<"1">>}, {val, 1}]},
        {row, [{key, 2}, {id, <<"2">>}, {val, 2}]},
        {row, [{key, 3}, {id, <<"3">>}, {val, 3}]},
        {row, [{key, 4}, {id, <<"4">>}, {val, 4}]},
        {row, [{key, 5}, {id, <<"5">>}, {val, 5}]},
        {row, [{key, 6}, {id, <<"6">>}, {val, 6}]},
        {row, [{key, 7}, {id, <<"7">>}, {val, 7}]},
        {row, [{key, 8}, {id, <<"8">>}, {val, 8}]},
        {row, [{key, 9}, {id, <<"9">>}, {val, 9}]},
        {row, [{key, 10}, {id, <<"10">>}, {val, 10}]}
    ]},
    etap:is(Result, Expect, "Simple view query worked.").

test_range(Db) ->
    Result = run_query(Db, [{start_key, 3}, {end_key, 5}]),
    Expect = {ok, [
        {total_and_offset, 10, 2},
        {row, [{key, 3}, {id, <<"3">>}, {val, 3}]},
        {row, [{key, 4}, {id, <<"4">>}, {val, 4}]},
        {row, [{key, 5}, {id, <<"5">>}, {val, 5}]}
    ]},
    etap:is(Result, Expect, "Query with range works.").


test_rev_range(Db) ->
    Result = run_query(Db, [
        {direction, rev},
        {start_key, 5}, {end_key, 3},
        {inclusive_end, true}
    ]),
    Expect = {ok, [
        {total_and_offset, 10, 5},
        {row, [{key, 5}, {id, <<"5">>}, {val, 5}]},
        {row, [{key, 4}, {id, <<"4">>}, {val, 4}]},
        {row, [{key, 3}, {id, <<"3">>}, {val, 3}]}
    ]},
    etap:is(Result, Expect, "Query with reversed range works.").

run_query(Db, Opts) ->
    couch_mrview:query_view(Db, <<"_design/bar">>, <<"baz">>, Opts).
