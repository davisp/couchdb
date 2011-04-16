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
-export([add/2, add_remove/3, modify/3, query_modify/4]).


-include("couch_db.hrl").


-record(stream_st, {
    dir,
    start_key,
    in_range,
    acc_fun,
    acc
}).

-record(rstream_st, {
    dir,
    start_key,
    end_key,
    in_range,
    grouped_key,
    grouped_kvs,
    grouped_reds,
    group_fun,
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
-define(in_red_range(S, K), (S#rstream_st.in_range)(K)).
-define(grouped(S, K), (S#rstream_st.group_fun)(S#rstream_st.grouped_key, K)).


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
        kv_node -> lookup_kv_node(Bt, ?l2t(NodeList), 1, Keys, [])
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


%% @doc Get the reduction of the entire btree.
%%
%% @spec full_reduce(Bt) -> {ok, Reduction}.
full_reduce(#btree{root=nil,reduce=Reduce}) ->
    {ok, Reduce(reduce, [])};
full_reduce(#btree{root={_P, Red}}) ->
    {ok, Red}.

%% @doc Complete a partial reduction.
%%
%% @spec final_reduce(Bt, Values) -> Reduction.
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

%% @doc Stream reductions from the btree.
%%
%% @spec fold_reduce(Bt, Fun, Acc, Options) -> {ok, Acc2}.
fold_reduce(#btree{root=Root}=Bt, Fun, Acc, Options) ->
    Dir = couch_util:get_value(dir, Options, fwd),
    StartKey0 = couch_util:get_value(start_key, Options),
    EndKey0 = case couch_util:get_value(end_key_gt, Options) of
        undefined -> couch_util:get_value(end_key, Options);
        EndKeyGt -> EndKeyGt
    end,
    {StartKey, EndKey} = case Dir of
        fwd -> {StartKey0, EndKey0};
        rev -> {EndKey0, StartKey0}
    end,
    DefGroupFun = fun(_, _) -> true end,
    State0 = #rstream_st{
        dir=Dir,
        start_key=StartKey,
        end_key=EndKey,
        in_range=make_range_fun(Bt, Dir, Options),
        grouped_key=undefined,
        grouped_kvs=[],
        grouped_reds=[],
        group_fun=couch_util:get_value(key_group_fun, Options, DefGroupFun),
        acc_fun=convert_arity(Fun),
        acc=Acc
    },
    try
        {ok, State} = red_stream_node(Bt, Root, State0),
        case State#rstream_st.grouped_key of
            undefined ->
                {ok, State#rstream_st.acc};
            GrpKey ->
                #rstream_st{grouped_kvs=GKVs, grouped_reds=GReds} = State,
                case Fun(GrpKey, {GKVs, GReds}, State#rstream_st.acc) of
                    {ok, Acc3} -> {ok, Acc3};
                    {stop, Acc3} -> {ok, Acc3}
                end
        end
    catch
        throw:{stop, AccDone} -> {ok, AccDone}
    end.

red_stream_node(_Bt, nil, State) ->
    {ok, State};
red_stream_node(Bt, {Ptr, _Red}, State) ->
    {NodeType, Nodes} = get_node(Bt, Ptr),
    case NodeType of
        kp_node -> red_stream_kp_node(Bt, Nodes, State);
        kv_node -> red_stream_kv_node(Bt, Nodes, State)
    end.

red_stream_kp_node(Bt, Nodes, State) ->
    Nodes0 = case State#rstream_st.start_key of
        undefined ->
            Nodes;
        StartKey ->
            DPred = fun({Key, _}) -> ?less(Bt, Key, StartKey) end,
            lists:dropwhile(DPred, Nodes)
    end,
    Nodes1 = case State#rstream_st.end_key of
        undefined ->
            Nodes0;
        EndKey ->
            SPred = fun({Key, _}) -> ?less(Bt, Key, EndKey) end,
            {InRange, MaybeInRange} = lists:splitwith(SPred, Nodes0),
            case MaybeInRange of
                [] -> InRange;
                [First | _] -> InRange ++ [First]
            end
    end,
    NodesInRange = adjust_dir(State#rstream_st.dir, Nodes1),
    red_stream_kp_node2(Bt, NodesInRange, State).

red_stream_kp_node2(Bt, [{_Key, PtrInfo} | RestNodes], State) when
                                State#rstream_st.grouped_key == undefined,
                                State#rstream_st.grouped_kvs == [],
                                State#rstream_st.grouped_reds == [] ->
    {ok, State2} = red_stream_node(Bt, PtrInfo, State),
    red_stream_kp_node2(Bt, RestNodes, State2);
red_stream_kp_node2(Bt, Nodes, State) ->
    Pred = fun({Key, _}) -> ?grouped(State, Key) end,
    {Grouped0, Ungrouped0} = lists:splitwith(Pred, Nodes),
    {GroupedNodes, UngroupedNodes} =
    case Grouped0 of
        [] ->
            {Grouped0, Ungrouped0};
        _ ->
            [FirstGrouped | RestGrouped] = lists:reverse(Grouped0),
            {RestGrouped, [FirstGrouped | Ungrouped0]}
    end,
    GroupedReds = [R || {_, {_,R}} <- GroupedNodes],
    State2 = State#rstream_st{
        grouped_reds=GroupedReds ++ State#rstream_st.grouped_reds
    },
    case UngroupedNodes of
        [{_Key, PtrInfo} | RestNodes] ->
            {ok, State3} = red_stream_node(Bt, PtrInfo, State2),
            red_stream_kp_node2(Bt, RestNodes, State3);
        [] ->
            {ok, State2}
    end.

red_stream_kv_node(Bt, KVs, State) ->
    KVs1 = case State#rstream_st.start_key of
        undefined ->
            KVs;
        StartKey ->
            DPred = fun({Key, _}) -> ?less(Bt, Key, StartKey) end,
            lists:dropwhile(DPred, KVs)
    end,
    KVs2 = case State#rstream_st.end_key of
        undefined ->
            KVs1;
        EndKey ->
            TPred = fun({Key, _}) -> not ?less(Bt, EndKey, Key) end,
            lists:takewhile(TPred, KVs1)
    end,
    KVsInRange = adjust_dir(State#rstream_st.dir, KVs2),
    red_stream_kv_node2(Bt, KVsInRange, State).

red_stream_kv_node2(_Bt, [], State) ->
    {ok, State};
red_stream_kv_node2(Bt, [{Key, Value}| RestKVs], State) ->
    case State#rstream_st.grouped_key of
        undefined ->
            State2 = State#rstream_st{
                grouped_key=Key,
                grouped_kvs=[?assemble(Bt, Key, Value)]
            },
            red_stream_kv_node2(Bt, RestKVs, State2);
        GroupedKey ->
            GroupedKVs = State#rstream_st.grouped_kvs,
            GroupedReds = State#rstream_st.grouped_reds,
            case ?grouped(State, Key) of
                true ->
                    State2 = State#rstream_st{
                        grouped_kvs=[?assemble(Bt, Key, Value) | GroupedKVs]
                    },
                    red_stream_kv_node2(Bt, RestKVs, State2);
                false ->
                    #rstream_st{acc_fun=AccFun, acc=Acc} = State,
                    case AccFun(GroupedKey, {GroupedKVs, GroupedReds}, Acc) of
                        {ok, Acc2} ->
                            State2 = State#rstream_st{
                                grouped_key=Key,
                                grouped_kvs=[?assemble(Bt, Key, Value)],
                                grouped_reds=[],
                                acc=Acc2
                            },
                            red_stream_kv_node2(Bt, RestKVs, State2);
                        {stop, Acc2} ->
                            throw({stop, Acc2})
                    end
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

%% @doc Modify values in the btree
%%
%% @spec modify(Bt, KeyFuns, InitAcc) -> {ok, Acc, NewBtree}.
modify(Bt, KeyFuns, Acc) ->
    #btree{root=Root} = Bt,
    Actions = [{modify, Key, Fun} || {Key, Fun} <- lists:sort(KeyFuns)],
    {ok, KeyPtrs, {[], Acc2}, Bt2} = modify_node(Bt, Root, Actions, {[], Acc}),
    {ok, NewRoot, Bt3} = complete_root(Bt2, KeyPtrs),
    {ok, Acc2, Bt3#btree{root=NewRoot}}.

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
    DelActions = [{remove, Key, nil} || Key <- Deletions],
    QryActions = [{lookup, Key, nil} || Key <- Queries],
    SortFun = fun({OpA, A, _}, {OpB, B, _}) ->
        case A == B of
            true -> op_order(OpA) < op_order(OpB);
            false -> ?less(Bt, A, B)
        end
    end,
    Actions = lists:sort(SortFun, InsActions ++ DelActions ++ QryActions),
    InitAcc = {[], nil},
    {ok, KeyPtrs, {Result, nil}, Bt2} = modify_node(Bt, Root, Actions, InitAcc),
    {ok, NewRoot, Bt3} = complete_root(Bt2, KeyPtrs),
    {ok, Result, Bt3#btree{root=NewRoot}}.

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
            {ok, KPs, Bt3} = write_node(Bt2, NodeType, NewNodes),
            {ok, KPs, Output2, Bt3}
    end.

modify_kp_node(Bt, {}, _Pos, Actions, [], Output) ->
    modify_node(Bt, nil, Actions, Output);
modify_kp_node(Bt, Nodes, Pos, [], NewNode, Output) ->
    {ok, ?rev2(NewNode, rest_nodes(Nodes, Pos, ?ts(Nodes), [])), Output, Bt};
modify_kp_node(Bt, Nodes, Pos, Actions, NewNode, Output) ->
    [{_, ActKey, _} | _]= Actions,
    NumNodes = ?ts(Nodes),
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
                not ?less(Bt, NodeKey, AKey)
            end,
            {LTEActs, GTActs} = lists:splitwith(SplitFun, Actions),
            {ok, KPs, Output2, Bt2} = modify_node(Bt, PtrInfo, LTEActs, Output),
            NewNode2 = ?rev2(KPs, rest_rev_nodes(Nodes, Pos, KpPos-1, NewNode)),
            modify_kp_node(Bt2, Nodes, KpPos + 1, GTActs, NewNode2, Output2)
    end.

modify_kv_node(Bt, Nodes, Pos, [], NewNode, Output) ->
    {ok, ?rev2(NewNode, rest_nodes(Nodes, Pos, ?ts(Nodes), [])), Output, Bt};
modify_kv_node(Bt, Nodes, Pos, Actions, NewNode, Output) when Pos > ?ts(Nodes)->
    [{ActType, ActKey, ActValue} | RestActs] = Actions,
    {Results, Acc} = Output,
    case ActType of
        insert ->
            NewNode2 = [{ActKey, ActValue} | NewNode],
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode2, Output);
        remove ->
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode, Output);
        modify ->
            {NewNode2, Acc2} = modify_value(Bt, ActValue, nil, Acc, NewNode),
            Output2 = {Results, Acc2},
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode2, Output2);
        lookup ->
            Output2 = {[{not_found, {ActKey, nil}} | Results], Acc},
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode, Output2)
    end;
modify_kv_node(Bt, Nodes, Pos, Actions, NewNode, Output) ->
    [{ActType, ActKey, ActValue} | RestActs] = Actions,
    {Result, Acc} = Output,
    KvPos = find_key_pos(Bt, Nodes, Pos, ?ts(Nodes), ActKey),
    {Key, Value} = element(KvPos, Nodes),
    NewNode2 =  rest_rev_nodes(Nodes, Pos, KvPos - 1, NewNode),
    case {cmp_keys(Bt, ActKey, Key), ActType} of
        {lesser, insert} ->
            NewNode3 = [{ActKey, ActValue} | NewNode2],
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode3, Output);
        {lesser, remove} ->
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode2, Output);
        {lesser, modify} ->
            {NewNode3, Acc2} = modify_value(Bt, ActValue, nil, Acc, NewNode2),
            Output2 = {Result, Acc2},
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode3, Output2);
        {lesser, lookup} ->
            Output2 = {[{not_found, {ActKey, nil}} | Result], Acc},
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode2, Output2);
        {equal, insert} ->
            NewNode3 = [{ActKey, ActValue} | NewNode2],
            modify_kv_node(Bt, Nodes, KvPos + 1, RestActs, NewNode3, Output);
        {equal, remove} ->
            modify_kv_node(Bt, Nodes, KvPos + 1, RestActs, NewNode2, Output);
        {equal, modify} ->
            KV = ?assemble(Bt, ActKey, Value),
            {NewNode3, Acc2} = modify_value(Bt, ActValue, KV, Acc, NewNode2),
            Output2 = {Result, Acc2},
            modify_kv_node(Bt, Nodes, KvPos + 1, RestActs, NewNode3, Output2);
        {equal, lookup} ->
            Output2 = {[{ok, ?assemble(Bt, Key, Value)} | Result], Acc},
            modify_kv_node(Bt, Nodes, KvPos, RestActs, NewNode2, Output2);
        {greater, _} ->
            NewNode3 = [{Key, Value} | NewNode2],
            modify_kv_node(Bt, Nodes, KvPos + 1, Actions, NewNode3, Output)
    end.

modify_value(Bt, ModFun, KV, Acc, ResultNode) ->
    case ModFun(KV, Acc) of
        {{insert, NewValue}, Acc2} ->
            {[?extract(Bt, NewValue) | ResultNode], Acc2};
        {remove, Acc2} ->
            {ResultNode, Acc2}
    end.

% Read/Write btree nodes.


%% @doc Read a node from disk.
%%
%% @spec get_node(Bt, Position) -> {NodeType, NodeList}.
get_node(#btree{fd = Fd}, NodePos) ->
    {ok, {NodeType, NodeList}} = couch_file:pread_term(Fd, NodePos),
    {NodeType, NodeList}.


%% @doc Write a btree node to disk.
%%
%% @spec write_node(Bt, NodeType, NodeList) -> {ok, KPs, Bt}.
write_node(Bt, NodeType, NodeList) ->
    NodeListList = chunkify(NodeList, Bt#btree.chunk_size),
    KPs = write_node(Bt, NodeType, NodeListList, []),
    {ok, KPs, Bt}.

write_node(_Bt, _NodeType, [], Acc) ->
    lists:reverse(Acc);
write_node(Bt, NodeType, [Node | Rest], Acc) ->
    {ok, Pointer} = couch_file:append_term(Bt#btree.fd, {NodeType, Node}),
    {LastKey, _} = lists:last(Node),
    KP = {LastKey, {Pointer, reduce_node(Bt, NodeType, Node)}},
    write_node(Bt, NodeType, Rest, [KP | Acc]).


%% @doc Break a node into pieces that are written to disk.
%%
%% This chunkify function sucks!
%%
%% It is inaccurate as it does not account for compression when blocks are
%% written. Plus with the "case byte_size(term_to_binary(InList)) of" code
%% it's probably really inefficient.
%%
%% @spec chunkify(InList, Threshold) -> [Node].
chunkify(InList, Threshold) ->
    case byte_size(term_to_binary(InList)) of
        Size when Size > Threshold ->
            NumberOfChunksLikely = ((Size div Threshold) + 1),
            ChunkThreshold = Size div NumberOfChunksLikely,
            chunkify(InList, ChunkThreshold, [], 0, []);
        _Else ->
            [InList]
    end.

chunkify([], _Threshold, [], 0, Acc) ->
    ?rev(Acc);
chunkify([], _Threshold, Buf, _BufSize, Acc) ->
    ?rev([?rev(Buf)] ++ Acc);
chunkify([Pair | Rest], Threshold, Buf, BufSize, Acc) ->
    case byte_size(term_to_binary(Pair)) of
        Size when (Size + BufSize) > Threshold andalso Buf /= [] ->
            chunkify(Rest, Threshold, [], 0, [?rev([Pair | Buf]) | Acc]);
        Size ->
            chunkify(Rest, Threshold, [Pair | Buf], BufSize + Size, Acc)
    end.


%% @doc Complete the top of a btree.
%%
%% @spec complete_root(Bt, NodeList) -> {ok, PointerInfo, Bt}.
complete_root(Bt, []) ->
    {ok, nil, Bt};
complete_root(Bt, [{_Key, PointerInfo}])->
    {ok, PointerInfo, Bt};
complete_root(Bt, KPs) ->
    {ok, ResultKeyPointers, Bt2} = write_node(Bt, kp_node, KPs),
    complete_root(Bt2, ResultKeyPointers).


% Utility Functions


%% @doc Adjust a list based on direction.
%%
%% @spec adjust_dir(Dir, List1) -> List2
adjust_dir(fwd, List) -> List;
adjust_dir(rev, List) -> ?rev(List).


%% @doc Prioritize modification actions
%%
%% This is used to sort operations on a given key when using the
%% query_modify function. This ordering allows us to do things
%% like, "get current value and delete from tree".
%%
%% @spec op_order(Op) -> Priority.
op_order(lookup) -> 1;
op_order(remove) -> 2;
op_order(insert) -> 3.


%% @doc Drop nodes while keeping reductions.
%%
%% @spec drop_nodes(Bt, Reds, StartKey, Nodes) -> {Reds, Rest}.
drop_nodes(_Bt, Reds, _StartKey, []) ->
    {Reds, []};
drop_nodes(Bt, Reds, StartKey, [{NodeKey, {Pointer, Red}} | RestKPs]) ->
    case ?less(Bt, NodeKey, StartKey) of
        true -> drop_nodes(Bt, [Red | Reds], StartKey, RestKPs);
        false -> {Reds, [{NodeKey, {Pointer, Red}} | RestKPs]}
    end.


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
            case ?less(Bt, B, A) of
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


%% @doc
%%
%% @spec reduce_node(Bt, NodeType, NodeList) -> Red.
reduce_node(#btree{reduce=nil}, _NodeType, _NodeList) ->
    [];
reduce_node(#btree{reduce=R}, kp_node, NodeList) ->
    R(rereduce, [Red || {_K, {_P, Red}} <- NodeList]);
reduce_node(#btree{reduce=R}=Bt, kv_node, NodeList) ->
    R(reduce, [?assemble(Bt, K, V) || {K, V} <- NodeList]).

