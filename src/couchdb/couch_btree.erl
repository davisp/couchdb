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
-export([add/2, add_remove/3, query_modify/4]).
-export([full_reduce/1, final_reduce/2, fold_reduce/4]).


-record(btree, {
    fd,
    root,
    extract_kv = fun({Key, Value}) -> {Key, Value} end,
    assemble_kv =  fun(Key, Value) -> {Key, Value} end,
    less = fun(A, B) -> A < B end,
    reduce = nil,
    chunk_size = 16#4FF
}).

-record(stream_st, {
    dir,
    start_key,
    in_range,
    acc_fun,
    acc
}).

%% Some helper macros
-define(ts(T), tuple_size(T)).
-define(l2t(L), list_to_tuple(L)).
-define(rev(L), lists:reverse(L)).
-define(rev2(L, T), lists:reverse(L, T)).
-define(extract(Bt, Value), (Bt#btree.extract_kv)(Value)).
-define(assemble(Bt, K, V), (Bt#btree.assemble_kv)(K, V)).
-define(less(Bt, A, B), (Bt#btree.less)(A, B)).
-define(in_range(S, K), (S#stream_st.in_range)(K)).

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
        kp_node -> lookup_kp_node(Bt, ?l2t(NodeList), 1, Keys, []);
        kv_mode -> lookup_kv_node(Bt, ?l2t(NodeList), 1, Keys, [])
    end.

lookup_kp_node(_Bt, _Nodes, _Pos, [], Output) ->
    {ok, lists:reverse(Output)};
lookup_kp_node(_Bt, Nodes, Pos, Keys, Output) when ?ts(Nodes) < Pos ->
    {ok, lists:reverse(Output, [{Key, not_found} || Key <- Keys])};
lookup_kp_node(Bt, Nodes, Pos, [NextKey | _] = Keys, Output) ->
    KeyPos = find_key_pos(Bt, Nodes, Pos, ?ts(Nodes), NextKey),
    {KpKey, PointerInfo} = element(KeyPos, Nodes),
    SplitFun = fun(LookupKey) -> not ?less(Bt, KpKey, LookupKey) end,
    case lists:splitwith(SplitFun, Keys) of
        {[], GreaterQueries} ->
            lookup_kp_node(Bt, Nodes, Pos + 1, GreaterQueries, Output);
        {LessEqQueries, GreaterQueries} ->
            {ok, Rows} = lookup(Bt, PointerInfo, LessEqQueries),
            Output2 = ?rev2(Rows, Output),
            lookup_kp_node(Bt, Nodes, KeyPos + 1, GreaterQueries, Output2)
    end.

lookup_kv_node(_Bt, _Nodes, _Pos, [], Output) ->
    {ok, lists:reverse(Output)};
lookup_kv_node(_Bt, Nodes, Pos, Keys, Output) when ?ts(Nodes) < Pos ->
    {ok, lists:reverse(Output, [{Key, not_found} || Key <- Keys])};
lookup_kv_node(Bt, Nodes, Pos, [NextKey | RestKeys], Output) ->
    KeyPos = find_key_pos(Bt, Nodes, Pos, ?ts(Nodes), NextKey),
    {KvKey, Val} = element(KeyPos, Nodes),
    case cmp_keys(Bt, NextKey, KvKey) of
        lesser ->
            Output2 = [{NextKey, not_found} | Output],
            lookup_kv_node(Bt, Nodes, Pos, RestKeys, Output2);
        equal ->
            Output2 = [{KvKey, {ok, ?assemble(Bt, KvKey, Val)}} | Output],
            lookup_kv_node(Bt, Nodes, Pos, RestKeys, Output2);
        greater ->
            Output2 = [{NextKey, not_found} | Output],
            lookup_kv_node(Bt, Nodes, Pos + 1, RestKeys, Output2)
    end.


%% @doc Fold over a btree in sorted order.
%%
%% @spec foldl(Bt, Fun, Acc) -> Acc2.
foldl(Bt, Fun, Acc) ->
    fold(Bt, Fun, Acc, []).

%% @doc Fold over a btree in sorted order.
%%
%% @spec foldl(Bt, Fun, Acc, Options) -> Acc2.
foldl(Bt, Fun, Acc, Options) ->
    fold(Bt, Fun, Acc, Options).

%% @doc Fold over a btree.
%% This will fold over an entire btree's key/value pairs passing
%% each pair to the provided function along with the given accumulator.
%%
%% Functions can return {ok, Acc2} to continue folding, or {stop, Acc2}
%% to halt the folding early.
fold(#btree{root=nil}, _Fun, Acc, _Options) ->
    {ok, {[], []}, Acc};
fold(#btree{root=Root}=Bt, Fun, Acc, Options) ->
    Dir = couch_util:get_value(dir, Options, fwd),
    State = #stream_st{
        dir=Dir,
        start_key=couch_util:get_value(start_key, Options),
        in_range=make_range_fun(Bt, Dir, Options),
        acc_fun=convert_arity(Fun),
        acc=Acc
    },
    case stream_node(Bt, [], Bt#btree.root, State) of
        {ok, Acc2}->
            {_P, FullReduction} = Root,
            {ok, {[], [FullReduction]}, Acc2};
        {stop, LastReduction, Acc2} ->
            {ok, LastReduction, Acc2}
    end.

stream_node(Bt, Reds, {Pointer, _Reds}, State) ->
    {NodeType, NodeList} = get_node(Bt, Pointer),
    AdjNodes = adjust_dir(State#stream_st.dir, NodeList),
    StartKey = State#stream_st.start_key,
    case {NodeType, StartKey} of
        {kp_node, _} -> stream_kp_node(Bt, Reds, AdjNodes, State);
        {kv_node, undefined} -> stream_kv_node(Bt, Reds, [], AdjNodes, State);
        {kv_node, _} -> stream_kv_node(Bt, Reds, AdjNodes, State)
    end.

stream_kp_node(_Bt, _Reds, [], State) ->
    {ok, State#stream_st.acc};
stream_kp_node(Bt, Reds, KPs, #stream_st{start_key=undefined}=State) ->
    [{_Key, {Ptr, Red}} | Rest] = KPs,
    case stream_node(Bt, Reds, {Ptr, Red}, State) of
        {ok, Acc2} ->
            stream_kp_node(Bt, [Red | Reds], Rest, State#stream_st{acc=Acc2});
        {stop, LastReds, Acc2} ->
            {stop, LastReds, Acc2}
    end;
stream_kp_node(Bt, Reds, KPs, State) ->
    StartKey = State#stream_st.start_key,
    {NewReds, NodesToStream} = case State#stream_st.dir of
        fwd ->
            drop_nodes(Bt, Reds, StartKey, KPs);
        rev ->
            RevKPs = lists:reverse(KPs),
            Predicate = fun({Key, _Ptr}) -> ?less(Bt, Key, StartKey) end,
            case lists:splitwith(Predicate, RevKPs) of
                {_RevsBefore, []} ->
                    {Reds, KPs};
                {RevBefore, [FirstAfter | Drop]} ->
                    Reds2 = [Red || {_K, {_P, Red}} <- Drop] ++ Reds,
                    {Reds2, [FirstAfter | ?rev(RevBefore)]}
            end
    end,
    case NodesToStream of
        [] ->
            {ok, State#stream_st.acc};
        [{_Key, {Ptr, Red}} | Rest] ->
            case stream_node(Bt, NewReds, {Ptr, Red}, State) of
                {ok, Acc2} ->
                    State2 = State#stream_st{start_key=undefined, acc=Acc2},
                    stream_kp_node(Bt, [Red | NewReds], Rest, State2);
                {stop, LastReds, Acc2} ->
                    {stop, LastReds, Acc2}
            end
    end.

stream_kv_node(Bt, Reds, KVs, State) ->
    StartKey = State#stream_st.start_key,
    DropFun = case State#stream_st.dir of
        fwd -> fun({Key, _}) -> ?less(Bt, Key, StartKey) end;
        rev -> fun({Key, _}) -> ?less(Bt, StartKey, Key) end
    end,
    {LTKVs, GTEKVs} = lists:splitwith(DropFun, KVs),
    AssembleLTKVs = [?assemble(Bt, K, V) || {K, V} <- LTKVs],
    stream_kv_node(Bt, Reds, AssembleLTKVs, GTEKVs, State).

stream_kv_node(_Bt, _Reds, _PrevKVs, [], State) ->
    {ok, State#stream_st.acc};
stream_kv_node(Bt, Reds, PrevKVs, [{K, V} | RestKVs], State) ->
    AccFun = State#stream_st.acc_fun,
    case ?in_range(State, K) of
        false ->
            {stop, {PrevKVs, Reds}, State#stream_st.acc};
        true ->
            AssembledKV = ?assemble(Bt, K, V),
            case AccFun(AssembledKV, {PrevKVs, Reds}, State#stream_st.acc) of
                {ok, Acc2} ->
                    PrevKVs2 = [AssembledKV | PrevKVs],
                    State2 = State#stream_st{acc=Acc2},
                    stream_kv_node(Bt, Reds, PrevKVs2, RestKVs, State2);
                {stop, Acc2} ->
                    {stop, {PrevKVs, Reds}, Acc2}
            end
    end.


%% @doc Add key/value pairs to the btree.
%%
%% @spec add(Bt, Insertions) -> {ok, Bt2}.
add(Bt, Insertions) ->
    add_remove(Bt, Insertions, []).

%% @doc Add and remove key/value pairs to the btree.
%%
%% @spec add_remove(Bt, Insertions, Deletions) -> {ok, Bt2}.
add_remove(Bt, Insertions, Deletions) ->
    {ok, [], Bt2} = query_modify(Bt, [], Insertions, Deletions),
    {ok, Bt2}.

%% @doc Add and remove key/value pairs and return query results from the btree.
%%
%% @spec query_modify(Bt, Queries, Insertions, Deletions) -> {ok, Results, Bt2}.
query_modify(Bt, Queries, Insertions, Deletions) ->
    #btree{root=Root} = Bt,
    InsActions = lists:map(
        fun(KeyValue) ->
            {Key, Value} = ?extract(Bt, KeyValue),
            {insert, Key, Value}
        end, Insertions),
    DelActions = [{delete, Key, nil} || Key <- Deletions],
    QryActions = [{lookup, Key, nil} || Key <- Queries],
    Actions = lists:append([InsActions, DelActions, QryActions]),
    SortFun = fun({OpA, A, _}, {OpB, B, _}) ->
        case A == B of
            true -> op_order(OpA) < op_order(OpB);
            false -> less(Bt, A, B)
        end
    end,
    Actions = lists:sort(SortFun, InsActions ++ DelActions ++ QryActions),
    {ok, KeyPointers, QueryResults, Bt2} = modify_node(Bt, Root, Actions, []),
    {ok, NewRoot, Bt3} = complete_root(Bt2, KeyPointers),
    {ok, QueryResults, Bt3#btree{root=NewRoot}}.

modify_node(Bt, PtrInfo, Actions, Output) ->
    {NodeType, NodeList} = case PtrInfo of
        nil -> {kv_node, []};
        {Ptr, _} -> get_node(Bt, Ptr)
    end,
    NodeTuple = ?l2t(NodeList),

    {ok, NewNodes, Output2, Bt2} = case NodeType of
        kp_node -> modify_kp_node(Bt, NodeTuple, 1, Actions, [], Output);
        kv_node -> modify_kv_node(Bt, NodeTuple, 1, Actions, [], Output)
    end,

    case NewNodes of
        [] ->
            {ok, [], Output2, Bt2};
        NodeList ->
            {LastKey, _LastValue} = element(?ts(NodeTuple), NodeTuple),
            {ok, [{LastKey, PtrInfo}], Output2, Bt2};
        _Else2 ->
            {ok, Root, Bt3} = write_node(Bt2, NodeType, NewNodes),
            {ok, Root, Output2, Bt3}
    end.

modify_kp_node(Bt, {}, _Pos, Actions, [], Output) ->
    modify_node(Bt, nil, Actions, Output);
modify_kp_node(Bt, Nodes, Pos, [], NewNode, Output) ->
    {ok, ?rev2(NewNode, rest_nodes(Nodes, Pos, ?ts(Nodes), [])), Output, Bt};
modify_kp_node(Bt, Nodes, Pos, Actions, NewNode, Output) ->
    [{_, ActKey, _} | _]= Actions,
    NumNodes = tuple_size(Nodes),
    KpPos = find_key_pos(Bt, Nodes, Pos, NumNodes, ActKey),
    case KpPos == NumNodes of
        true  ->
            {_, PtrInfo} = element(NumNodes, Nodes),
            {ok, KPs, Output2, Bt2} = modify_node(Bt, PtrInfo, Actions, Output),
            NewNode2 = ?rev2(NewNode, rest_nodes(Nodes, Pos, NumNodes-1, KPs)),
            {ok, NewNode2, Output2, Bt2};
        false ->
            {NodeKey, PtrInfo} = element(KpPos, Nodes),
            SplitFun = fun({_AType, AKey, _AValue}) ->
                not less(Bt, NodeKey, AKey)
            end,
            {LTEActs, GTActs} = lists:splitwith(SplitFun, Actions),
            {ok, KPs, Output2, Bt2} = modify_node(Bt, PtrInfo, LTEActs, GTActs),
            NewNode2 = ?rev2(KPs, rest_nodes(Nodes, Pos, KpPos - 1, NewNode)),
            modify_kp_node(Bt2, Nodes, KpPos + 1, GTActs, NewNode2, Output2)
    end.

modify_kv_node(Bt, Nodes, Pos, [], NewNode, Output) ->
    {ok, ?rev2(NewNode, rest_nodes(Nodes, Pos, ?ts(Nodes), [])), Output, Bt};
modify_kv_node(Bt, Nodes, Pos, Actions, NewNode, Output) when Pos > ?ts(Nodes) ->
    [{ActType, ActKey, ActValue} | RestActs] = Actions,
    case ActType of
        insert ->
            NewNode2 = [{ActKey, ActValue} | NewNode],
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode2, Output);
        remove ->
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode, Output);
        fetch ->
            Output2 = [{not_found, {ActKey, nil}} | Output],
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode, Output2)
    end;
modify_kv_node(Bt, Nodes, Pos, Actions, NewNode, Output) ->
    [{ActType, ActKey, ActValue} | RestActs] = Actions,
    KvPos = find_key_pos(Bt, Nodes, Pos, ?ts(Nodes), ActKey),
    {Key, Value} = element(KvPos, Nodes),
    NewNode2 =  rest_rev_nodes(Nodes, Pos, KvPos - 1, NewNode),
    case {cmp_keys(Bt, ActKey, Key), ActType} of
        {lesser, insert} ->
            NewNode3 = [{ActKey, ActValue} | NewNode2],
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode3, Output);
        {lesser, delete} ->
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode2, Output);
        {lesser, lookup} ->
            Output2 = [{not_found, {ActKey, nil}} | Output],
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode2, Output2);
        {equal, insert} ->
            NewNode3 = [{ActKey, ActValue} | NewNode2],
            modify_kv_node(Bt, Nodes, KvPos + 1, RestActs, NewNode3, Output);
        {equal, delete} ->
            modify_kv_node(Bt, Nodes, KvPos + 1, RestActs, NewNode2, Output);
        {equal, lookup} ->
            Output2 = [{ok, ?assemble(Bt, Key, Value)} | Output],
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode2, Output2);
        {greater, _} ->
            NewNode3 = [{Key, Value} | NewNode2],
            modify_kv_node(Bt, Nodes, KvPos + 1, Actions, NewNode3, Output)
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

assemble(#btree{assemble_kv=Assemble}, Key, Value) ->
    Assemble(Key, Value).
less(#btree{less=Less}, A, B) ->
    Less(A, B).


%% @doc
%%
%% @spec
reorder_results(Keys, SortedResults) when length(Keys) < 100 ->
    [couch_util:get_value(Key, SortedResults) || Key <- Keys];
reorder_results(Keys, SortedResults) ->
    KeyDict = dict:from_list(SortedResults),
    [dict:fetch(Key, KeyDict) || Key <- Keys].


%% @doc Find the node for a given key.
%%
%% This does a binary search over the Nodes tuple to find
%% the node where a given would be located.
%%
%% @spec find_key_pos(Bt, Nodes, Start, End, Key) -> Position.
find_key_pos(_Bt, _Nodes, Start, End, _Key) when Start == End ->
    End;
find_key_pos(Bt, Nodes, Start, End, Key) ->
    Mid = Start + ((End - Start) div 2),
    {NodeKey, _} = element(Mid, Nodes),
    case ?less(Bt, NodeKey, Key) of
        true -> find_key_pos(Bt, Nodes, Mid+1, End, Key);
        false -> find_key_pos(Bt, Nodes, Start, Mid, Key)
    end.


%% @doc Wrap 2-arity functions so they can ignore reductions.
%%
%% @spec convert_arity(Fun1) -> Fun2.
convert_arity(Fun) when is_function(Fun, 2) ->
    fun(KV, _Reds, AccIn) -> Fun(KV, AccIn) end;
convert_arity(Fun) when is_function(Fun, 3) ->
    Fun.


%% @doc
%%
%% @spec
make_range_fun(#btree{less=Less}, Dir, Options) ->
    case couch_util:get_value(end_key_gt, Options) of
        undefined ->
            case couch_util:get_value(end_key, Options) of
                undefined ->
                    fun(_Key) -> true end;
                LastKey ->
                    case Dir of
                        fwd -> fun(Key) -> not Less(LastKey, Key) end;
                        rev -> fun(Key) -> not Less(Key, LastKey) end
                    end
                end;
        EndKey ->
            case Dir of
                fwd -> fun(Key) -> Less(Key, EndKey) end;
                rev -> fun(Key) -> Less(EndKey, Key) end
            end
    end.


%% @doc
%%
%% @spec
cmp_keys(Bt, A, B) ->
    case ?less(Bt, A, B) of
        true ->
            lesser;
        false ->
            case ?less(Bt, A, B) of
                true ->
                    greater;
                false ->
                    equal
            end
    end.


%% @doc
%%
%% @spec
rest_nodes(Nodes, Start, End, Tail) ->
    rest_nodes(Nodes, Start, End, [], Tail).

rest_nodes(_Nodes, Start, End, Acc, Tail) when Start > End ->
    ?rev2(Acc, Tail);
rest_nodes(Nodes, Start, End, Acc, Tail) ->
    rest_nodes(Nodes, Start + 1, End, [element(Start, Nodes) | Acc], Tail).


%% @doc
%%
%% @spec
rest_rev_nodes(_Nodes, Start, End, Tail) when Start > End ->
    Tail;
rest_rev_nodes(Nodes, Start, End, Tail) ->
    rest_rev_nodes(Nodes, Start + 1, End, [element(Start, Nodes) | Tail]).
























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




