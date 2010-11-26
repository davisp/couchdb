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

filename() -> test_util:build_file("test/etap/temp.022").
rows() -> 250.

-record(btree, {
    fd,
    root,
    extract_kv,
    assemble_kv,
    less,
    reduce,
    chunk_size,
    buffer
}).

main(_) ->
    test_util:init_code_path(),
    etap:plan(unknown),
    case (catch test()) of
        ok ->
            etap:end_tests();
        {'EXIT', {Type, StackTrace}} ->
            etap:diag("Test died abnormally: ~p", [Type]),
            etap:diag("Stack: ~p", [StackTrace]),
            timer:sleep(500),
            etap:bail()
    end,
    ok.

test()->
    Sorted = [{Seq, random:uniform()} || Seq <- lists:seq(1, rows())],
    etap:diag("Testing sorted keys."),
    test_kvs(Sorted),
    etap:diag("Testing reverse sorted keys."),
    test_kvs(lists:reverse(Sorted)),
    etap:diag("Testing shuffled keys."),
    test_kvs(shuffle(Sorted)),
    ok.

test_kvs(KeyValues) ->
    ReduceFun = fun
        (reduce, KVs) -> length(KVs);
        (rereduce, Reds) -> lists:sum(Reds)
    end,

    Keys = [K || {K, _} <- KeyValues],

    {ok, Fd} = couch_file:open(filename(), [create, overwrite]),
    {ok, Bt0} = couch_btree:open(nil, Fd, [{reduce, ReduceFun}, buffer]),
    etap:is(Bt0#btree.buffer, true, "Buffering option was set."),

    {ok, Bt1} = couch_btree:add(Bt0, KeyValues),
    {ok, Bt2} = couch_btree:flush(Bt1),
    check_btree(Bt2, lists:sort(KeyValues), "All keys inserted."),

    {ok, Bt3} = couch_btree:add_remove(Bt2, [], Keys),
    check_btree(Bt3, [], "All keys removed."),

    FoldFun = fun(KV, {AccBt, Acc}) ->
        {ok, AccBt2} = couch_btree:add(AccBt, [KV]),
        {ok, AccBt3} = couch_btree:flush(AccBt2),
        Acc2 = [KV | Acc],
        check_btree(AccBt3, lists:sort(Acc2)),
        {AccBt3, Acc2}
    end,
    {Bt4, _} = lists:foldl(FoldFun, {Bt3, []}, KeyValues),
    check_btree(Bt4, lists:sort(KeyValues)),

    ok.

check_btree(Bt, Sorted, Mesg) ->
    etap:is(check_btree(Bt, Sorted), [], Mesg).
    
check_btree(Bt, Sorted) ->
    FoldFun = fun({Key, Val}, [{Key, Val} | Rest]) -> {ok, Rest} end,
    Resp = couch_btree:foldl(Bt, FoldFun, Sorted),
    {ok, _Reds, Acc} = couch_btree:foldl(Bt, FoldFun, Sorted),
    Acc.

shuffle(List) ->
    randomize(round(math:log(length(List)) + 0.5), List).

randomize(1, List) ->
    randomize(List);
randomize(T, List) ->
    FoldFun = fun(_E, Acc) -> randomize(Acc) end,
    lists:foldl(FoldFun, randomize(List), lists:seq(1, (T - 1))).

randomize(List) ->
    D = lists:map(fun(A) -> {random:uniform(), A} end, List),
    {_, D1} = lists:unzip(lists:keysort(1, D)),
    D1.
