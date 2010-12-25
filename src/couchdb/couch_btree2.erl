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

-module(couch_btree2).


-export([open/1, open/2, open/3, set_options/2, get_state/1]).
-export([add/2, add_remove/3, query_modify/4]).


-include("couch_db.hrl").


-record(info, {key, pos, num, red}).


%% Some helper macros
-define(MIN_SZ, 510).
-define(MAX_SZ, 1020).


-define(ts(T), tuple_size(T)).
-define(l2t(L), list_to_tuple(L)).
-define(rev(L), lists:reverse(L)).
-define(rev2(L, T), lists:reverse(L, T)).
-define(extract(Bt, Value), (Bt#btree.extract_kv)(Value)).
-define(assemble(Bt, K, V), (Bt#btree.assemble_kv)(K, V)).
-define(less(Bt, A, B), (Bt#btree.less)(A, B)).
-define(in_range(S, K), (S#stream_st.in_range)(K)).
-define(in_red_range(S, K), (S#rstream_st.in_range)(K)).
-define(grouped(S, K), (S#rstream_st.group_fun)(S#rstream_st.grouped_key, K)).


%% @doc Open a new btree.
open(Fd) ->
    open(Fd, nil).

%% @doc Open an existing btree.
open(Fd, State) ->
    open(Fd, State, []).

%% @doc Open an existing btree.
%%
%% Multiple btrees can be stored in a single file.
open(Fd, State, Options) ->
    Bt = set_options(#btree{fd=Fd}, Options),
    Bt2 = case State of
        nil -> Bt#btree{root=nil};
        Pos -> Bt#btree{root=get_info(Bt, Pos)}
    end,
    {ok, Bt2}.


%% @doc Specify options for a given btree.
%%
%% You should ensure that you use the same options each time
%% you open the btree.
set_options(Bt, Options) ->
    lists:foldl(fun(Option, Bt2) ->
        case Option of
            {split, Extract} -> Bt2#btree{extract_kv=Extract};
            {join, Assemble} -> Bt2#btree{assemble_kv=Assemble};
            {less, Less} -> Bt2#btree{less=Less};
            {reduce, Reduce} -> Bt2#btree{reduce=Reduce};
            {chunk_size, Size} -> Bt2#btree{chunk_size=Size};
            _ -> Bt2
        end
    end, Bt, Options).


%% @doc Get the current state of the btree.
%%
%% This term is usually stored in a file that will be read
%% later to reopen the btree.
get_state(#btree{root=Root}) ->
    Root.


%% @doc Add key/value pairs to the btree.
add(Bt, Insertions) ->
    add_remove(Bt, Insertions, []).

%% @doc Add and remove key/value pairs to the btree.
add_remove(Bt, Insertions, Deletions) ->
    {ok, [], Bt2} = query_modify(Bt, [], Insertions, Deletions),
    {ok, Bt2}.

%% @doc Add and remove key/value pairs and return query results from the btree.
query_modify(Bt, Queries, Insertions, Deletions) ->
    #btree{root=Root} = Bt,
    InsActions = lists:map(fun(KeyValue) ->
        {Key, Value} = ?extract(Bt, KeyValue),
        {insert, Key, Value}
    end, Insertions),
    DelActions = [{delete, Key, nil} || Key <- Deletions],
    QryActions = [{lookup, Key, nil} || Key <- Queries],
    SortFun = fun
        ({insert, A, _}, {insert, A, _}) ->
            erlang:error({duplicate_insert_key, A});
        ({OpA, A, _}, {OpB, B, _}) ->
            case A == B of
                true -> op_order(OpA) =< op_order(OpB);
                false -> ?less(Bt, A, B)
            end
    end,
    Actions = lists:usort(SortFun, InsActions ++ DelActions ++ QryActions),
    {NewInfos, QueryResults} = modify_node(Bt, Root, Actions, []),
    {ok, NewRoot} = case NewInfos of
        [] -> {ok, nil};
        [Info] -> write_info(Info);
        Infos -> write_info(write_node(p, Bt, Infos))
    end,
    {ok, QueryResults, Bt#btree{root=NewRoot}}.

modify_node(Bt, Info, Actions, Output) ->
    {NodeType, NodeList} = case PtrInfo of
        nil -> {v, []};
        Pos -> get_node(Bt, Info#info.pos)
    end,
    NodeTuple = ?l2t(NodeList),

    {NewNodeList0, Output2} = case NodeType of
        p -> modify_kp_node(Bt, NodeTuple, 1, Actions, [], Output);
        v -> modify_kv_node(Bt, NodeTuple, 1, Actions, [], Output)
    end,

    case NewNodeList of
        [] -> {[], Output2};
        NodeList -> {Info, Output2};
        _ -> {write_node(NodeType, Bt2, NewNodeList), Output2}
    end.

modify_kp_node(Bt, {}, _Pos, Actions, [], Output) ->
    modify_node(Bt, nil, Actions, Output);
modify_kp_node(Bt, Nodes, Pos, [], NewNode, Output) ->
    {ok, ?rev2(NewNode, rest_nodes(Nodes, Pos, ?ts(Nodes), [])), Output, Bt};
modify_kp_node(Bt, Nodes, Pos, Actions, NewNode, Output) ->
    [{_, ActKey, _} | _]= Actions,
    NumNodes = ?ts(Nodes),
    {KpPos, {KpKey, PtrInfo}} = find_key_pos(Bt, Nodes, Pos, NumNodes, ActKey),
    case KpPos == NumNodes of
        true  ->
            {KPs, Output2} = modify_node(Bt, PtrInfo, Actions, Output),
            NewNode2 = ?rev2(NewNode, rest_nodes(Nodes, Pos, NumNodes-1, KPs)),
            {?l2t(NewNode2), Output2};
        false ->
            SplitFun = fun({_AType, AKey, _AValue}) ->
                not ?less(Bt, KpKey, AKey)
            end,
            {LTEActs, GTActs} = lists:splitwith(SplitFun, Actions),
            {KPs, Output2} = modify_node(Bt, PtrInfo, LTEActs, Output),
            NewNode2 = ?rev2(KPs, rest_rev_nodes(Nodes, Pos, KpPos-1, NewNode)),
            modify_kp_node(Bt, Nodes, KpPos + 1, GTActs, NewNode2, Output2)
    end.

modify_kv_node(Bt, Nodes, Pos, [], NewNode, Output) ->
    {?rev2(NewNode, rest_nodes(Nodes, Pos, ?ts(Nodes), [])), Output};
modify_kv_node(Bt, Nodes, Pos, Actions, NewNode, Output) when Pos > ?ts(Nodes)->
    [{ActType, ActKey, ActValue} | RestActs] = Actions,
    case ActType of
        insert ->
            NewNode2 = [{insert, {ActKey, ActValue}} | NewNode],
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode2, Output);
        remove ->
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode, Output);
        lookup ->
            Output2 = [{not_found, {ActKey, nil}} | Output],
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode, Output2)
    end;
modify_kv_node(Bt, Nodes, Pos, Actions, NewNode, Output) ->
    [{ActType, ActKey, ActValue} | RestActs] = Actions,
    {KvPos, {Key, Value}} = find_key_pos(Bt, Nodes, Pos, ?ts(Nodes), ActKey),
    NewNode2 =  rest_rev_nodes(Nodes, Pos, KvPos - 1, NewNode),
    case {cmp_keys(Bt, ActKey, Key), ActType} of
        {lesser, insert} ->
            NewNode3 = [{insert, {ActKey, ActValue}} | NewNode2],
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode3, Output);
        {lesser, delete} ->
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode2, Output);
        {lesser, lookup} ->
            Output2 = [{not_found, {ActKey, nil}} | Output],
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode2, Output2);
        {equal, insert} ->
            NewNode3 = [{insert, {ActKey, ActValue}} | NewNode2],
            modify_kv_node(Bt, Nodes, KvPos + 1, RestActs, NewNode3, Output);
        {equal, delete} ->
            modify_kv_node(Bt, Nodes, KvPos + 1, RestActs, NewNode2, Output);
        {equal, lookup} ->
            Output2 = [{ok, ?assemble(Bt, Key, Value)} | Output],
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode2, Output2);
        {greater, _} ->
            NewNode3 = [element(KvPos, Nodes) | NewNode2],
            modify_kv_node(Bt, Nodes, KvPos + 1, Actions, NewNode3, Output)
    end.


% Read/Write btree nodes.


%% @doc Read a node from disk.
%%
%% @spec get_node(Bt, Position) -> {NodeType, NodeList}.
get_node(#btree{fd = Fd}, NodePos) ->
    {ok, {NodeType, NodeList}} = couch_file:pread_term(Fd, NodePos),
    {NodeType, NodeList}.


%% @doc Write a btree node to disk.
write_node(p, Bt, NodeList0) ->
    NodeList = rebalance_nodes(Bt, NodeList0, []),
    {LastKey, Num, Infos} = flush_kp_node(Bt, NodeList, nil, 0, []),
    {ok, Pos} = couch_file:append_term(Bt#btree.fd, {p, Infos}),
    #info{key=LastKey, pos=Pos, num=Num};
write_node(v, Bt, NodeList) ->
    {LastKey, Num, Infos} = flush_kv_node(Bt, NodeList, nil, 0, []),
    {ok, Pos} = couch_file:append_term(Bt#btree.fd, {v, KVPtrs}),
    #info{key=LastKey, pos=Pos, num=Num}.


get_info(_Bt, #info{}=Info) ->
    Info;
get_info(Bt, Ptr) ->
    {ok, {Key, Ptr, Num, Red}} = couch_file:pread_term(Bt#btree.fd, Ptr),
    #info{key=Key, ptr=Ptr, num=Num, red=Red}.


write_info(Bt, #info{key=Key, pos=Pos, num=Num, red=undefined}) ->
    {ok, {Type, Nodes}} = couch_file:pread_term(Bt#btree.fd, Info#info.pos),
    Red = case Type of
        p -> reduce_kp_node(Bt, Nodes, []);
        v -> reduce_kv_node(Bt, Nodes, [])
    end,
    couch_file:append_term(Bt#btree.fd, {Key, Pos, Num, Red}).


rebalance_nodes(Bt, [], Acc) ->
    ?rev(Acc);
rebalance_nodes(Bt, [#info{}=Info], []) ->
    case Info#info.num >= ?MAX_SZ of
        true ->
            {Type, Nodes} = get_node(Bt, Info#info.pos),
            split_nodes(Bt, Type, Info#info.num, Nodes);
        false ->
            [Info]
    end;
rebalance_nodes(Bt, [Info | Rest], []) ->
    rebalance_nodes(Bt, Rest, [Info]);
rebalance_nodes(Bt, [LInfo | RestL], [RInfo | RestR]) ->
    LInfo = get_info(Left),
    RInfo = get_info(Right),
    NewInfos = case needs_balance(Bt, LInfo) or needs_balance(Bt, RInfo) of
        false ->
            [LInfo, RInfo];
        true -> 
            {ok, {Type, Left}} = couch_file:pread_term(Bt#btree.fd, LPtr),
            {ok, {Type, Right}} = couch_file:pread_term(Bt#btree.fd, RPtr),
            split_nodes(Bt, Type, LCnt + RCnt, Left ++ Right)
    end.
    rebalance_nodes(Bt, RestN, NewInfos ++ RestA).


split_nodes(Bt, Type, Num, Nodes) when Num =< ?MAX_SZ ->
    {ok, Pos} = couch_file:append_term(Bt#btree.fd, {Type, Nodes}),
    Info = get_info(lists:last(Nodes)),
    [Info#{pos=Pos, num=Num, red=undefined}];
split_nodes(Bt, Type, Num, Nodes) ->
    Left, Right = lists:split(Num / 2, Nodes),
    LInfo = split_nodes(Bt, Type, lists:length(Left), Left),
    RInfo = split_nodes(Bt, Type, lists:length(Right), Right),
    [LInfo, RInfo].
    

needs_balance(_Bt, Info) ->
    Info#info.num =< ?MIN_SZ or Info#info.num >= ?MAX_SZ.


flush_kp_node(_Bt, [], Info, Num, Acc) ->
    Info1 = get_info(Bt, Info),
    {Info1#info.key, Num, ?rev(Acc)};
flush_kp_node(_Bt, [#info{}=Info | Rest], Last, Num, Acc) ->
    {ok, Pos} = write_info(NewInfo),
    flush_kp_node(Bt, Rest, NewInfo, Num + 1, [Pos | Acc]);
flush_kp_node(_Bt, [Pos | Rest], Last, Num, Acc) when is_integer(Pos) ->
    flush_kp_node(Bt, Rest, Pos, Num + 1, [Pos | Acc]).


flush_kv_node(_Bt, [], Info, Num, Acc) ->
    Info1 = get_info(Bt, Info),
    {Info1#info.key, Num, ?rev(Acc)};
flush_kv_node(Bt, [{K, V} | Rest], Num, Acc) ->
    {ok, Pos} = couch_file:append_term(Bt#btree.fd, {K, V}),
    flush_kv_node(Bt, Rest, Info, Num + 1, [Pos | Acc]);
flush_kv_node(Bt, [Pos | Rest], Num, Acc) when is_integer(Pos) ->
    flush_kv_node(Bt, Rest, Pos, Num + 1, [Pos | Acc]).


reduce_kp_node(Bt, [], Reds) ->
    final_reudce(Bt, {[], Reds});
reduce_kp_node(Bt, NodeList, Reds) when length(Reds) >= 64 ->
    Red = final_reduce(Bt, {[], Reds}),
    reduce_kp_node(Bt, NodeList, [Red]);
reduce_kp_node(Bt, [Info | Rest], Reds) ->
    Info1 = get_info(Bt, Info),
    reduce_kp_node(Bt, Rest, [Info1#info.red | Reds]).


reduce_kv_node(Bt, [], KVs, Reds) ->
    final_reduce(Bt, {KVs, Reds});
reduce_kv_node(Bt, NodeList, KVs, Reds) when length(KVs) >= 64 ->
    Red = final_reduce(Bt, {KVs, []}),
    reduce_kv_node(Bt, NodeList, [], [Red | Reds]);
reduce_kv_node(Bt, [{K, V} | Rest], KVs, Reds) ->
    reduce_kv_node(Bt, Rest, [{K, V} | KVs], Reds);
reduce_kv_node(Bt, [Pos | Rest], KVs, Reds) ->
    {ok, {K, V}} = couch_file:pread_term(Bt#btree.fd, Pos),
    reduce_kv_node(Bt, Rest, [{K, V} | KVs], Reds).


%% @doc Complete a partial reduction.
final_reduce(#btree{reduce=Reduce}, Val) ->
    final_reduce(Reduce, Val);
final_reduce(Reduce, {[], []}) ->
    Reduce(reduce, []);
final_reduce(_Bt, {[], [Red]}) ->
    Red;
final_reduce(Reduce, {[], Reductions}) ->
    Reduce(rereduce, Reductions);
final_reduce(Reduce, {KVs, Reductions}) ->
    Red = Reduce(reduce, KVs),
    final_reduce(Reduce, {[], [Red | Reductions]}).


% Utility Functions


%% @doc Prioritize modification actions
%%
%% This is used to sort operations on a given key when using the
%% query_modify function. This ordering allows us to do things
%% like, "get current value and delete from tree".
op_order(fetch) -> 1;
op_order(remove) -> 2;
op_order(insert) -> 3.


%% @doc Find the node for a given key.
%%
%% This does a binary search over the Nodes tuple to find
%% the node where a given would be located.
find_key_pos(Bt, Nodes, Start, End, Key) ->
    {ok, StartInfo} = couch_file:pread_term(Bt#btree.fd, Start),
    {ok, EndInfo} = couch_file:pread_term(Bt#btree.fd, End),
    find_key_pos(Bt, Nodes, Start, StartInfo, End, EndInfo, Key).

find_key_pos(_Bt, _Nodes, Start, _, End, EndInfo, _Key) when Start == End ->
    {End, EndInfo};
find_key_pos(Bt, Nodes, Start, End, Key) ->
    Mid = Start + ((End - Start) div 2),
    {ok, MidInfo} = couch_file:pread_term(Bt#btree.fd, element(Mid, Nodes)),
    NodeKey = element(1, MidInfo),
    case ?less(Bt, NodeKey, Key) of
        true ->
            Mid2Pos = element(Mid+1, Nodes),
            {ok, MidInfo2} = couch_file:pread_term(Bt#btree.fd, Mid2Pos),
            find_key_pos(Bt, Nodes, Mid+1, MidInfo2, End, EndInfo, Key);
        false ->
            find_key_pos(Bt, Nodes, Start, StartInfo, Mid, MidInfo, Key)
    end.


%% @doc Returns lesser, equal, or greater
cmp_keys(Bt, A, B) ->
    case ?less(Bt, A, B) of
        true ->
            lesser;
        false ->
            case ?less(Bt, B, A) of
                true ->
                    greater;
                false ->
                    equal
            end
    end.


%% @doc
rest_nodes(Nodes, Start, End, Tail) ->
    rest_nodes(Nodes, Start, End, [], Tail).

rest_nodes(_Nodes, Start, End, Acc, Tail) when Start > End ->
    ?rev2(Acc, Tail);
rest_nodes(Nodes, Start, End, Acc, Tail) ->
    rest_nodes(Nodes, Start + 1, End, [element(Start, Nodes) | Acc], Tail).


%% @doc
rest_rev_nodes(_Nodes, Start, End, Tail) when Start > End ->
    Tail;
rest_rev_nodes(Nodes, Start, End, Tail) ->
    rest_rev_nodes(Nodes, Start + 1, End, [element(Start, Nodes) | Tail]).


%% @doc
reduce_node(#btree{reduce=nil}, _NodeType, _NodeList) ->
    [];
reduce_node(#btree{reduce=R}, kp_node, NodeList) ->
    R(rereduce, [Red || {_K, {_P, Red}} <- NodeList]);
reduce_node(#btree{reduce=R}=Bt, kv_node, NodeList) ->
    R(reduce, [?assemble(Bt, K, V) || {K, V} <- NodeList]).

