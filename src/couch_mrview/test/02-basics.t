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
    Result = couch_mrview:query_view(Db, <<"_design/bar">>, <<"baz">>),
    etap:is(ok, check_result(Result), "Basic view result ok."),
    ok.

check_result({ok, [{total_and_offset, 10, 0} | Rest]}) ->
    check_result(1, Rest, 0).

check_result(_, [], Num) when Num == 10 ->
    ok;
check_result(Key, [{row, Row} | Rest], Num) ->
    Id = list_to_binary(integer_to_list(Key)),
    {value, {id, Id}} = lists:keysearch(id, 1, Row),
    {value, {key, Key}} = lists:keysearch(key, 1, Row),
    {value, {val, Key}} = lists:keysearch(val, 1, Row),
    check_result(Key+1, Rest, Num+1).
    
