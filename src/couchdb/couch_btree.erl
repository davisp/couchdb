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

-module(couch_btree).


-export([open/1, open/2, open/3, set_options/2, get_state/1]).
-export([lookup/2, foldl/3, foldl/4, fold/4]).
-export([full_reduce/1, final_reduce/2, fold_reduce/4]).
-export([add/2, add_remove/3, query_modify/4]).


-record(btree,{
    fd,
    root,
    extract_kv = fun({Key, Value}) -> {Key, Value} end,
    assemble_kv =  fun(Key, Value) -> {Key, Value} end,
    less = fun(A, B) -> A < B end,
    reduce = nil,
    chunk_size = 16#4FF
}).


%% Some helper macros
-define(l2t(L), list_to_tuple(L)).
-define(rev(L), lists:reverse(L)).
-define(rev2(L, T), lists:reverse(L, T)).
-define(extract(Bt, Value), (Bt#btree.extract_kv)(Value)).
-define(assemble(Bt, K, V), (Bt#btree.assemble_kv)(K, V)).
-define(less(Bt, A, B), (Bt#btree.less)(A, B)).


%% @doc Open a new btree.
%%
%% @spec open(Fd:file()) -> btree()
open(Fd) ->
    open(nil, Fd).

%% @doc Open an existing btree.
%%
%% @spec open(State, Fd:file()) -> btree()
open(State, Fd) ->
    open(State, Fd, []).

%% @doc Open an existing btree.
%%
%% Multiple btrees can be stored in a single file.
%%
%% @spec open(State, Fd:file(), Options) -> btree()
%% @type State. An opaque term returned from get_state/1.
%% @type Options = [{Key, Value}]. A list of btree options.
open(State, Fd, Options) ->
    {ok, set_options(#btree{root=State, fd=Fd}, Options)}.


%% @doc Specify options for a given btree.
%%
%% You should ensure that you use the same options each time
%% you open the btree.
%%
%% @spec set_options(btree(), Options) -> btree()
%% @type Options = [{atom(), any()}].
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
%%
%% @spec get_state(btree()) -> btstate().
%% @type btstate. An opaque term specifying the btree state.
get_state(#btree{root=Root}) ->
    Root.


%% @doc Lookup a list of keys in the btree.
%% Results are returned in the same order as
%% the keys were specified.
%%
%% @spec lookup(btree(), Keys) -> [{Key, Value | not_found}]
%% @type btree(). A btree record returned by couch_btree:open/3.
%% @type Keys -> [any()]
%% @type Key -> any()
lookup(#btree{root=Root, less=Less}=Bt, Keys) ->
    SortedKeys = lists:sort(Less, Keys),
    {ok, SortedResults} = lookup(Bt, Root, SortedKeys),
    reorder_results(Keys, SortedResults).

lookup(_Bt, nil, Keys) ->
    {ok, [{Key, not_found} || Key <- Keys]};
lookup(Bt, {Pointer, _Reds}, Keys) ->
    {NodeType, NodeList} = get_node(Bt, Pointer),
    case NodeType of
        kp_node -> lookup_kpnode(Bt, ?l2t(NodeList), 1, Keys, []);
        kv_mode -> lookup_kvnode(Bt, ?l2t(NodeList), 1, Keys, [])
    end.

lookup_kpnode(_Bt, _Nodes, _Pos, [], Output) ->
    {ok, lists:reverse(Output)};
lookup_kpnode(_Bt, Nodes, Pos, Keys, Output) when tuple_size(Nodes) < Pos ->
    {ok, lists:reverse(Output, [{Key, not_found} || Key <- Keys])};
lookup_kpnode(Bt, Nodes, Pos, [NextKey | _] = Keys, Output) ->
    KeyPos = find_key_pos(Bt, Nodes, Pos, tuple_size(Nodes), NextKey),
    {KpKey, PointerInfo} = element(KeyPos, Nodes),
    SplitFun = fun(LookupKey) -> not ?less(Bt, KpKey, LookupKey) end,
    case lists:splitwith(SplitFun, Keys) of
        {[], GreaterQueries} ->
            lookup_kpnode(Bt, Nodes, Pos + 1, GreaterQueries, Output);
        {LessEqQueries, GreaterQueries} ->
            {ok, Rows} = lookup(Bt, PointerInfo, LessEqQueries),
            Output2 = ?rev2(Rows, Output),
            lookup_kpnode(Bt, Nodes, KeyPos + 1, GreaterQueries, Output2)
    end.

lookup_kvnode(_Bt, _Nodes, _Pos, [], Output) ->
    {ok, lists:reverse(Output)};
lookup_kvnode(_Bt, Nodes, Pos, Keys, Output) when tuple_size(Nodes) < Pos ->
    {ok, lists:reverse(Output, [{Key, not_found} || Key <- Keys])};
lookup_kvnode(Bt, Nodes, Pos, [NextKey | RestKeys], Output) ->
    KeyPos = find_key_pos(Bt, Nodes, Pos, tuple_size(Nodes), NextKey),
    {KvKey, Val} = element(KeyPos, Nodes),
    case ?less(Bt, NextKey, KvKey) of
        true ->
            Output2 = [{NextKey, not_found} | Output],
            lookup_kvnode(Bt, Nodes, Pos, RestKeys, Output2);
        false ->
            case ?less(Bt, KvKey, NextKey) of
                true ->
                    Output2 = [{NextKey, not_found} | Output],
                    lookup_kvnode(Bt, Nodes, Pos + 1, RestKeys, Output2);
                false ->
                    Result = {ok, ?assemble(Bt, KvKey, Val)},
                    Output2 = [{KvKey, Result} | Output],
                    lookup_kvnode(Bt, Nodes, Pos, RestKeys, Output2)
            end
    end.


foldl(Bt, Fun, Acc) ->
    fold(Bt, Fun, Acc, []).


foldl(Bt, Fun, Acc, Options) ->
    fold(Bt, Fun, Acc, Options).


fold(#btree{root=nil}, _Fun, Acc, _Options) ->
    {ok, {[], []}, Acc};
fold(#btree{root=Root}=Bt, Fun, Acc, Options) ->
    Dir = couch_util:get_value(dir, Options, fwd),
    InRange = make_key_in_end_range_function(Bt, Dir, Options),
    Result =
    case couch_util:get_value(start_key, Options) of
    undefined ->
        stream_node(Bt, [], Bt#btree.root, InRange, Dir,
                convert_fun_arity(Fun), Acc);
    StartKey ->
        stream_node(Bt, [], Bt#btree.root, StartKey, InRange, Dir,
                convert_fun_arity(Fun), Acc)
    end,
    case Result of
    {ok, Acc2}->
        {_P, FullReduction} = Root,
        {ok, {[], [FullReduction]}, Acc2};
    {stop, LastReduction, Acc2} ->
        {ok, LastReduction, Acc2}
    end.


stream_node(Bt, Reds, {Pointer, _Reds}, StartKey, InRange, Dir, Fun, Acc) ->
    {NodeType, NodeList} = get_node(Bt, Pointer),
    case NodeType of
    kp_node ->
        stream_kp_node(Bt, Reds, adjust_dir(Dir, NodeList), StartKey, InRange, Dir, Fun, Acc);
    kv_node ->
        stream_kv_node(Bt, Reds, adjust_dir(Dir, NodeList), StartKey, InRange, Dir, Fun, Acc)
    end.


stream_node(Bt, Reds, {Pointer, _Reds}, InRange, Dir, Fun, Acc) ->
    {NodeType, NodeList} = get_node(Bt, Pointer),
    case NodeType of
    kp_node ->
        stream_kp_node(Bt, Reds, adjust_dir(Dir, NodeList), InRange, Dir, Fun, Acc);
    kv_node ->
        stream_kv_node2(Bt, Reds, [], adjust_dir(Dir, NodeList), InRange, Dir, Fun, Acc)
    end.


stream_kp_node(_Bt, _Reds, [], _InRange, _Dir, _Fun, Acc) ->
    {ok, Acc};
stream_kp_node(Bt, Reds, [{_Key, {Pointer, Red}} | Rest], InRange, Dir, Fun, Acc) ->
    case stream_node(Bt, Reds, {Pointer, Red}, InRange, Dir, Fun, Acc) of
    {ok, Acc2} ->
        stream_kp_node(Bt, [Red | Reds], Rest, InRange, Dir, Fun, Acc2);
    {stop, LastReds, Acc2} ->
        {stop, LastReds, Acc2}
    end.


stream_kp_node(Bt, Reds, KPs, StartKey, InRange, Dir, Fun, Acc) ->
    {NewReds, NodesToStream} =
    case Dir of
    fwd ->
        % drop all nodes sorting before the key
        drop_nodes(Bt, Reds, StartKey, KPs);
    rev ->
        % keep all nodes sorting before the key, AND the first node to sort after
        RevKPs = lists:reverse(KPs),
         case lists:splitwith(fun({Key, _Pointer}) -> less(Bt, Key, StartKey) end, RevKPs) of
        {_RevsBefore, []} ->
            % everything sorts before it
            {Reds, KPs};
        {RevBefore, [FirstAfter | Drop]} ->
            {[Red || {_K,{_P,Red}} <- Drop] ++ Reds,
                 [FirstAfter | lists:reverse(RevBefore)]}
        end
    end,
    case NodesToStream of
    [] ->
        {ok, Acc};
    [{_Key, {Pointer, Red}} | Rest] ->
        case stream_node(Bt, NewReds, {Pointer, Red}, StartKey, InRange, Dir, Fun, Acc) of
        {ok, Acc2} ->
            stream_kp_node(Bt, [Red | NewReds], Rest, InRange, Dir, Fun, Acc2);
        {stop, LastReds, Acc2} ->
            {stop, LastReds, Acc2}
        end
    end.


stream_kv_node(Bt, Reds, KVs, StartKey, InRange, Dir, Fun, Acc) ->
    DropFun =
    case Dir of
    fwd ->
        fun({Key, _}) -> less(Bt, Key, StartKey) end;
    rev ->
        fun({Key, _}) -> less(Bt, StartKey, Key) end
    end,
    {LTKVs, GTEKVs} = lists:splitwith(DropFun, KVs),
    AssembleLTKVs = [assemble(Bt,K,V) || {K,V} <- LTKVs],
    stream_kv_node2(Bt, Reds, AssembleLTKVs, GTEKVs, InRange, Dir, Fun, Acc).


stream_kv_node2(_Bt, _Reds, _PrevKVs, [], _InRange, _Dir, _Fun, Acc) ->
    {ok, Acc};
stream_kv_node2(Bt, Reds, PrevKVs, [{K,V} | RestKVs], InRange, Dir, Fun, Acc) ->
    case InRange(K) of
    false ->
        {stop, {PrevKVs, Reds}, Acc};
    true ->
        AssembledKV = assemble(Bt, K, V),
        case Fun(AssembledKV, {PrevKVs, Reds}, Acc) of
        {ok, Acc2} ->
            stream_kv_node2(Bt, Reds, [AssembledKV | PrevKVs], RestKVs, InRange, Dir, Fun, Acc2);
        {stop, Acc2} ->
            {stop, {PrevKVs, Reds}, Acc2}
        end
    end.


full_reduce(#btree{root=nil,reduce=Reduce}) ->
    {ok, Reduce(reduce, [])};
full_reduce(#btree{root={_P, Red}}) ->
    {ok, Red}.


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


fold_reduce(#btree{root=Root}=Bt, Fun, Acc, Options) ->
    Dir = couch_util:get_value(dir, Options, fwd),
    StartKey = couch_util:get_value(start_key, Options),
    EndKey = couch_util:get_value(end_key, Options),
    KeyGroupFun = couch_util:get_value(key_group_fun, Options, fun(_,_) -> true end),
    {StartKey2, EndKey2} =
    case Dir of
        rev -> {EndKey, StartKey};
        fwd -> {StartKey, EndKey}
    end,
    try
        {ok, Acc2, GroupedRedsAcc2, GroupedKVsAcc2, GroupedKey2} =
            reduce_stream_node(Bt, Dir, Root, StartKey2, EndKey2, undefined, [], [],
            KeyGroupFun, Fun, Acc),
        if GroupedKey2 == undefined ->
            {ok, Acc2};
        true ->
            case Fun(GroupedKey2, {GroupedKVsAcc2, GroupedRedsAcc2}, Acc2) of
            {ok, Acc3} -> {ok, Acc3};
            {stop, Acc3} -> {ok, Acc3}
            end
        end
    catch
        throw:{stop, AccDone} -> {ok, AccDone}
    end.

reduce_stream_node(_Bt, _Dir, nil, _KeyStart, _KeyEnd, GroupedKey, GroupedKVsAcc,
        GroupedRedsAcc, _KeyGroupFun, _Fun, Acc) ->
    {ok, Acc, GroupedRedsAcc, GroupedKVsAcc, GroupedKey};
reduce_stream_node(Bt, Dir, {P, _R}, KeyStart, KeyEnd, GroupedKey, GroupedKVsAcc,
        GroupedRedsAcc, KeyGroupFun, Fun, Acc) ->
    case get_node(Bt, P) of
    {kp_node, NodeList} ->
        reduce_stream_kp_node(Bt, Dir, NodeList, KeyStart, KeyEnd, GroupedKey,
                GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, Acc);
    {kv_node, KVs} ->
        reduce_stream_kv_node(Bt, Dir, KVs, KeyStart, KeyEnd, GroupedKey,
                GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, Acc)
    end.


reduce_stream_kp_node(Bt, Dir, NodeList, KeyStart, KeyEnd,
                        GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
                        KeyGroupFun, Fun, Acc) ->
    Nodes =
    case KeyStart of
    undefined ->
        NodeList;
    _ ->
        lists:dropwhile(
            fun({Key,_}) ->
                less(Bt, Key, KeyStart)
            end, NodeList)
    end,
    NodesInRange =
    case KeyEnd of
    undefined ->
        Nodes;
    _ ->
        {InRange, MaybeInRange} = lists:splitwith(
            fun({Key,_}) ->
                less(Bt, Key, KeyEnd)
            end, Nodes),
        InRange ++ case MaybeInRange of [] -> []; [FirstMaybe|_] -> [FirstMaybe] end
    end,
    reduce_stream_kp_node2(Bt, Dir, adjust_dir(Dir, NodesInRange), KeyStart, KeyEnd,
        GroupedKey, GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, Acc).


reduce_stream_kp_node2(Bt, Dir, [{_Key, NodeInfo} | RestNodeList], KeyStart, KeyEnd,
                        undefined, [], [], KeyGroupFun, Fun, Acc) ->
    {ok, Acc2, GroupedRedsAcc2, GroupedKVsAcc2, GroupedKey2} =
            reduce_stream_node(Bt, Dir, NodeInfo, KeyStart, KeyEnd, undefined,
                [], [], KeyGroupFun, Fun, Acc),
    reduce_stream_kp_node2(Bt, Dir, RestNodeList, KeyStart, KeyEnd, GroupedKey2,
            GroupedKVsAcc2, GroupedRedsAcc2, KeyGroupFun, Fun, Acc2);
reduce_stream_kp_node2(Bt, Dir, NodeList, KeyStart, KeyEnd,
        GroupedKey, GroupedKVsAcc, GroupedRedsAcc, KeyGroupFun, Fun, Acc) ->
    {Grouped0, Ungrouped0} = lists:splitwith(fun({Key,_}) ->
        KeyGroupFun(GroupedKey, Key) end, NodeList),
    {GroupedNodes, UngroupedNodes} =
    case Grouped0 of
    [] ->
        {Grouped0, Ungrouped0};
    _ ->
        [FirstGrouped | RestGrouped] = lists:reverse(Grouped0),
        {RestGrouped, [FirstGrouped | Ungrouped0]}
    end,
    GroupedReds = [R || {_, {_,R}} <- GroupedNodes],
    case UngroupedNodes of
    [{_Key, NodeInfo}|RestNodes] ->
        {ok, Acc2, GroupedRedsAcc2, GroupedKVsAcc2, GroupedKey2} =
            reduce_stream_node(Bt, Dir, NodeInfo, KeyStart, KeyEnd, GroupedKey,
                GroupedKVsAcc, GroupedReds ++ GroupedRedsAcc, KeyGroupFun, Fun, Acc),
        reduce_stream_kp_node2(Bt, Dir, RestNodes, KeyStart, KeyEnd, GroupedKey2,
                GroupedKVsAcc2, GroupedRedsAcc2, KeyGroupFun, Fun, Acc2);
    [] ->
        {ok, Acc, GroupedReds ++ GroupedRedsAcc, GroupedKVsAcc, GroupedKey}
    end.


reduce_stream_kv_node(Bt, Dir, KVs, KeyStart, KeyEnd,
                        GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
                        KeyGroupFun, Fun, Acc) ->

    GTEKeyStartKVs =
    case KeyStart of
    undefined ->
        KVs;
    _ ->
        lists:dropwhile(fun({Key,_}) -> less(Bt, Key, KeyStart) end, KVs)
    end,
    KVs2 =
    case KeyEnd of
    undefined ->
        GTEKeyStartKVs;
    _ ->
        lists:takewhile(
            fun({Key,_}) ->
                not less(Bt, KeyEnd, Key)
            end, GTEKeyStartKVs)
    end,
    reduce_stream_kv_node2(Bt, adjust_dir(Dir, KVs2), GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
                        KeyGroupFun, Fun, Acc).


reduce_stream_kv_node2(_Bt, [], GroupedKey, GroupedKVsAcc, GroupedRedsAcc,
        _KeyGroupFun, _Fun, Acc) ->
    {ok, Acc, GroupedRedsAcc, GroupedKVsAcc, GroupedKey};
reduce_stream_kv_node2(Bt, [{Key, Value}| RestKVs], GroupedKey, GroupedKVsAcc,
        GroupedRedsAcc, KeyGroupFun, Fun, Acc) ->
    case GroupedKey of
    undefined ->
        reduce_stream_kv_node2(Bt, RestKVs, Key,
                [assemble(Bt,Key,Value)], [], KeyGroupFun, Fun, Acc);
    _ ->

        case KeyGroupFun(GroupedKey, Key) of
        true ->
            reduce_stream_kv_node2(Bt, RestKVs, GroupedKey,
                [assemble(Bt,Key,Value)|GroupedKVsAcc], GroupedRedsAcc, KeyGroupFun,
                Fun, Acc);
        false ->
            case Fun(GroupedKey, {GroupedKVsAcc, GroupedRedsAcc}, Acc) of
            {ok, Acc2} ->
                reduce_stream_kv_node2(Bt, RestKVs, Key, [assemble(Bt,Key,Value)],
                    [], KeyGroupFun, Fun, Acc2);
            {stop, Acc2} ->
                throw({stop, Acc2})
            end
        end
    end.


add(Bt, InsertKeyValues) ->
    add_remove(Bt, InsertKeyValues, []).

add_remove(Bt, InsertKeyValues, RemoveKeys) ->
    {ok, [], Bt2} = query_modify(Bt, [], InsertKeyValues, RemoveKeys),
    {ok, Bt2}.

query_modify(Bt, LookupKeys, InsertValues, RemoveKeys) ->
    #btree{root=Root} = Bt,
    InsertActions = lists:map(
        fun(KeyValue) ->
            {Key, Value} = extract(Bt, KeyValue),
            {insert, Key, Value}
        end, InsertValues),
    RemoveActions = [{remove, Key, nil} || Key <- RemoveKeys],
    FetchActions = [{fetch, Key, nil} || Key <- LookupKeys],
    SortFun =
        fun({OpA, A, _}, {OpB, B, _}) ->
            case A == B of
            % A and B are equal, sort by op.
            true -> op_order(OpA) < op_order(OpB);
            false ->
                less(Bt, A, B)
            end
        end,
    Actions = lists:sort(SortFun, lists:append([InsertActions, RemoveActions, FetchActions])),
    {ok, KeyPointers, QueryResults, Bt2} = modify_node(Bt, Root, Actions, []),
    {ok, NewRoot, Bt3} = complete_root(Bt2, KeyPointers),
    {ok, QueryResults, Bt3#btree{root=NewRoot}}.


modify_node(Bt, RootPointerInfo, Actions, QueryOutput) ->
    case RootPointerInfo of
    nil ->
        NodeType = kv_node,
        NodeList = [];
    {Pointer, _Reds} ->
        {NodeType, NodeList} = get_node(Bt, Pointer)
    end,
    NodeTuple = list_to_tuple(NodeList),

    {ok, NewNodeList, QueryOutput2, Bt2} =
    case NodeType of
    kp_node -> modify_kpnode(Bt, NodeTuple, 1, Actions, [], QueryOutput);
    kv_node -> modify_kvnode(Bt, NodeTuple, 1, Actions, [], QueryOutput)
    end,
    case NewNodeList of
    [] ->  % no nodes remain
        {ok, [], QueryOutput2, Bt2};
    NodeList ->  % nothing changed
        {LastKey, _LastValue} = element(tuple_size(NodeTuple), NodeTuple),
        {ok, [{LastKey, RootPointerInfo}], QueryOutput2, Bt2};
    _Else2 ->
        {ok, ResultList, Bt3} = write_node(Bt2, NodeType, NewNodeList),
        {ok, ResultList, QueryOutput2, Bt3}
    end.


modify_kpnode(Bt, {}, _LowerBound, Actions, [], QueryOutput) ->
    modify_node(Bt, nil, Actions, QueryOutput);
modify_kpnode(Bt, NodeTuple, LowerBound, [], ResultNode, QueryOutput) ->
    {ok, lists:reverse(ResultNode, bounded_tuple_to_list(NodeTuple, LowerBound,
            tuple_size(NodeTuple), [])), QueryOutput, Bt};
modify_kpnode(Bt, NodeTuple, LowerBound,
        [{_, FirstActionKey, _}|_]=Actions, ResultNode, QueryOutput) ->
    Sz = tuple_size(NodeTuple),
    N = find_key_pos(Bt, NodeTuple, LowerBound, Sz, FirstActionKey),
    case N =:= Sz of
    true  ->
        % perform remaining actions on last node
        {_, PointerInfo} = element(Sz, NodeTuple),
        {ok, ChildKPs, QueryOutput2, Bt2} =
            modify_node(Bt, PointerInfo, Actions, QueryOutput),
        NodeList = lists:reverse(ResultNode, bounded_tuple_to_list(NodeTuple, LowerBound,
            Sz - 1, ChildKPs)),
        {ok, NodeList, QueryOutput2, Bt2};
    false ->
        {NodeKey, PointerInfo} = element(N, NodeTuple),
        SplitFun = fun({_ActionType, ActionKey, _ActionValue}) ->
                not less(Bt, NodeKey, ActionKey)
            end,
        {LessEqQueries, GreaterQueries} = lists:splitwith(SplitFun, Actions),
        {ok, ChildKPs, QueryOutput2, Bt2} =
                modify_node(Bt, PointerInfo, LessEqQueries, QueryOutput),
        ResultNode2 = lists:reverse(ChildKPs, bounded_tuple_to_revlist(NodeTuple,
                LowerBound, N - 1, ResultNode)),
        modify_kpnode(Bt2, NodeTuple, N+1, GreaterQueries, ResultNode2, QueryOutput2)
    end.

modify_kvnode(Bt, NodeTuple, LowerBound, [], ResultNode, QueryOutput) ->
    {ok, lists:reverse(ResultNode, bounded_tuple_to_list(NodeTuple, LowerBound, tuple_size(NodeTuple), [])), QueryOutput, Bt};
modify_kvnode(Bt, NodeTuple, LowerBound, [{ActionType, ActionKey, ActionValue} | RestActions], ResultNode, QueryOutput) when LowerBound > tuple_size(NodeTuple) ->
    case ActionType of
    insert ->
        modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput);
    remove ->
        % just drop the action
        modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, ResultNode, QueryOutput);
    fetch ->
        % the key/value must not exist in the tree
        modify_kvnode(Bt, NodeTuple, LowerBound, RestActions, ResultNode, [{not_found, {ActionKey, nil}} | QueryOutput])
    end;
modify_kvnode(Bt, NodeTuple, LowerBound, [{ActionType, ActionKey, ActionValue} | RestActions], AccNode, QueryOutput) ->
    N = find_key_pos(Bt, NodeTuple, LowerBound, tuple_size(NodeTuple), ActionKey),
    {Key, Value} = element(N, NodeTuple),
    ResultNode =  bounded_tuple_to_revlist(NodeTuple, LowerBound, N - 1, AccNode),
    case less(Bt, ActionKey, Key) of
    true ->
        case ActionType of
        insert ->
            % ActionKey is less than the Key, so insert
            modify_kvnode(Bt, NodeTuple, N, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput);
        remove ->
            % ActionKey is less than the Key, just drop the action
            modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, QueryOutput);
        fetch ->
            % ActionKey is less than the Key, the key/value must not exist in the tree
            modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, [{not_found, {ActionKey, nil}} | QueryOutput])
        end;
    false ->
        % ActionKey and Key are maybe equal.
        case less(Bt, Key, ActionKey) of
        false ->
            case ActionType of
            insert ->
                modify_kvnode(Bt, NodeTuple, N+1, RestActions, [{ActionKey, ActionValue} | ResultNode], QueryOutput);
            remove ->
                modify_kvnode(Bt, NodeTuple, N+1, RestActions, ResultNode, QueryOutput);
            fetch ->
                % ActionKey is equal to the Key, insert into the QueryOuput, but re-process the node
                % since an identical action key can follow it.
                modify_kvnode(Bt, NodeTuple, N, RestActions, ResultNode, [{ok, assemble(Bt, Key, Value)} | QueryOutput])
            end;
        true ->
            modify_kvnode(Bt, NodeTuple, N + 1, [{ActionType, ActionKey, ActionValue} | RestActions], [{Key, Value} | ResultNode], QueryOutput)
        end
    end.


% Read/Write btree nodes.


get_node(#btree{fd = Fd}, NodePos) ->
    {ok, {NodeType, NodeList}} = couch_file:pread_term(Fd, NodePos),
    {NodeType, NodeList}.

write_node(Bt, NodeType, NodeList) ->
    % split up nodes into smaller sizes
    NodeListList = chunkify(NodeList, Bt#btree.chunk_size),
    % now write out each chunk and return the KeyPointer pairs for those nodes
    ResultList = [
        begin
            {ok, Pointer} = couch_file:append_term(Bt#btree.fd, {NodeType, ANodeList}),
            {LastKey, _} = lists:last(ANodeList),
            {LastKey, {Pointer, reduce_node(Bt, NodeType, ANodeList)}}
        end
    ||
        ANodeList <- NodeListList
    ],
    {ok, ResultList, Bt}.


% Utility Functions

extract(#btree{extract_kv=Extract}, Value) ->
    Extract(Value).
assemble(#btree{assemble_kv=Assemble}, Key, Value) ->
    Assemble(Key, Value).
less(#btree{less=Less}, A, B) ->
    Less(A, B).

find_key_pos(_Bt, _Nodes, Start, End, _Key) when Start == End ->
    End;
find_key_pos(Bt, Tuple, Start, End, Key) ->
    Mid = Start + ((End - Start) div 2),
    {TupleKey, _} = element(Mid, Tuple),
    case less(Bt, TupleKey, Key) of
        true -> find_key_pos(Bt, Tuple, Mid+1, End, Key);
        false -> find_key_pos(Bt, Tuple, Start, Mid, Key)
    end.






% linear search is faster for small lists, length() is 0.5 ms for 100k list
reorder_results(Keys, SortedResults) when length(Keys) < 100 ->
    [couch_util:get_value(Key, SortedResults) || Key <- Keys];
reorder_results(Keys, SortedResults) ->
    KeyDict = dict:from_list(SortedResults),
    [dict:fetch(Key, KeyDict) || Key <- Keys].

















% wraps a 2 arity function with the proper 3 arity function
convert_fun_arity(Fun) when is_function(Fun, 2) ->
    fun(KV, _Reds, AccIn) -> Fun(KV, AccIn) end;
convert_fun_arity(Fun) when is_function(Fun, 3) ->
    Fun.    % Already arity 3

make_key_in_end_range_function(#btree{less=Less}, fwd, Options) ->
    case couch_util:get_value(end_key_gt, Options) of
    undefined ->
        case couch_util:get_value(end_key, Options) of
        undefined ->
            fun(_Key) -> true end;
        LastKey ->
            fun(Key) -> not Less(LastKey, Key) end
        end;
    EndKey ->
        fun(Key) -> Less(Key, EndKey) end
    end;
make_key_in_end_range_function(#btree{less=Less}, rev, Options) ->
    case couch_util:get_value(end_key_gt, Options) of
    undefined ->
        case couch_util:get_value(end_key, Options) of
        undefined ->
            fun(_Key) -> true end;
        LastKey ->
            fun(Key) -> not Less(Key, LastKey) end
        end;
    EndKey ->
        fun(Key) -> Less(EndKey, Key) end
    end.





% for ordering different operations with the same key.
% fetch < remove < insert
op_order(fetch) -> 1;
op_order(remove) -> 2;
op_order(insert) -> 3.



complete_root(Bt, []) ->
    {ok, nil, Bt};
complete_root(Bt, [{_Key, PointerInfo}])->
    {ok, PointerInfo, Bt};
complete_root(Bt, KPs) ->
    {ok, ResultKeyPointers, Bt2} = write_node(Bt, kp_node, KPs),
    complete_root(Bt2, ResultKeyPointers).

%%%%%%%%%%%%% The chunkify function sucks! %%%%%%%%%%%%%
% It is inaccurate as it does not account for compression when blocks are
% written. Plus with the "case byte_size(term_to_binary(InList)) of" code
% it's probably really inefficient.

chunkify(InList, Threshold) ->
    case byte_size(term_to_binary(InList)) of
    Size when Size > Threshold ->
        NumberOfChunksLikely = ((Size div Threshold) + 1),
        ChunkThreshold = Size div NumberOfChunksLikely,
        chunkify(InList, ChunkThreshold, [], 0, []);
    _Else ->
        [InList]
    end.

chunkify([], _ChunkThreshold, [], 0, OutputChunks) ->
    lists:reverse(OutputChunks);
chunkify([], _ChunkThreshold, OutList, _OutListSize, OutputChunks) ->
    lists:reverse([lists:reverse(OutList) | OutputChunks]);
chunkify([InElement | RestInList], ChunkThreshold, OutList, OutListSize, OutputChunks) ->
    case byte_size(term_to_binary(InElement)) of
    Size when (Size + OutListSize) > ChunkThreshold andalso OutList /= [] ->
        chunkify(RestInList, ChunkThreshold, [], 0, [lists:reverse([InElement | OutList]) | OutputChunks]);
    Size ->
        chunkify(RestInList, ChunkThreshold, [InElement | OutList], OutListSize + Size, OutputChunks)
    end.



reduce_node(#btree{reduce=nil}, _NodeType, _NodeList) ->
    [];
reduce_node(#btree{reduce=R}, kp_node, NodeList) ->
    R(rereduce, [Red || {_K, {_P, Red}} <- NodeList]);
reduce_node(#btree{reduce=R}=Bt, kv_node, NodeList) ->
    R(reduce, [assemble(Bt, K, V) || {K, V} <- NodeList]).






bounded_tuple_to_revlist(_Tuple, Start, End, Tail) when Start > End ->
    Tail;
bounded_tuple_to_revlist(Tuple, Start, End, Tail) ->
    bounded_tuple_to_revlist(Tuple, Start+1, End, [element(Start, Tuple)|Tail]).

bounded_tuple_to_list(Tuple, Start, End, Tail) ->
    bounded_tuple_to_list2(Tuple, Start, End, [], Tail).

bounded_tuple_to_list2(_Tuple, Start, End, Acc, Tail) when Start > End ->
    lists:reverse(Acc, Tail);
bounded_tuple_to_list2(Tuple, Start, End, Acc, Tail) ->
    bounded_tuple_to_list2(Tuple, Start + 1, End, [element(Start, Tuple) | Acc], Tail).













adjust_dir(fwd, List) ->
    List;
adjust_dir(rev, List) ->
    lists:reverse(List).


drop_nodes(_Bt, Reds, _StartKey, []) ->
    {Reds, []};
drop_nodes(Bt, Reds, StartKey, [{NodeKey, {Pointer, Red}} | RestKPs]) ->
    case less(Bt, NodeKey, StartKey) of
    true -> drop_nodes(Bt, [Red | Reds], StartKey, RestKPs);
    false -> {Reds, [{NodeKey, {Pointer, Red}} | RestKPs]}
    end.




