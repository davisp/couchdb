#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./src/couchdb -sasl errlog_type error -boot start_sasl -noshell

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

filename() -> "./test/etap/temp.022".
group() -> 10.
rows() -> 1000.
total() -> group() * rows().

main(_) ->
    test_util:init_code_path(),
    etap:plan(unknown),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            timer:sleep(333),
            etap:bail()
    end,
    ok.

test()->
    {ok, Fd} = couch_file:open(filename(), [create,overwrite]),

    test_basic(Fd),

    couch_file:close(Fd).

test_basic(Fd) ->
    {ok, Ems} = couch_emsort:open(Fd, [{count, 256}]),
    Ems1 = lists:foldl(fun(_, EAcc0) ->
       KVs = [{random:uniform(), nil} || _ <- lists:seq(1, group())],
       {ok, EAcc1} = couch_emsort:add(EAcc0, KVs),
       EAcc1
    end, Ems, lists:seq(1, rows())),

    FoldFun = fun(K, nil, {Prev, Bad, Count}) ->
        case K < Prev of
            true -> {K, Bad+1, Count+1};
            false -> {K, Bad, Count+1}
        end
    end,

    {_, BadOrder, TotalRows} = couch_emsort:sort(Ems1, FoldFun, {-1, 0, 0}),
    etap:is(BadOrder, 0, "No rows were found out of order."),
    etap:is(TotalRows, total(), "Found exactly as many rows as expected."),
    ok.


