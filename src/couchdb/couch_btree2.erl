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
-export([well_formed/1]).

-include("couch_db.hrl").


-record(info, {type, key, pos, num, red}).


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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Update/Delete functions %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


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
    RootInfo = get_info(Bt, Root),
    {NewInfo, QueryResults} = modify_node(Bt, RootInfo, Actions, []),
    {ok, NewRoot} = case NewInfo of
        nil -> {ok, nil};
        _ -> complete_root(Bt, NewInfo)
    end,
    {ok, QueryResults, Bt#btree{root=NewRoot}}.

modify_node(Bt, Info, Actions, Output) ->
    {NodeType, NodeList} = case Info of
        nil -> {v, []};
        _ -> get_node(Bt, Info#info.pos)
    end,
    NodeTuple = ?l2t(NodeList),

    {NewNodeList, Output2} = case NodeType of
        p -> modify_kp_node(Bt, NodeTuple, 1, Actions, [], Output);
        v -> modify_kv_node(Bt, NodeTuple, 1, Actions, [], Output)
    end,

    case NewNodeList of
        [] -> {nil, Output2};
        NodeList -> {Info, Output2};
        _ -> {write_node(NodeType, Bt, NewNodeList), Output2}
    end.

modify_kp_node(Bt, {}, _Pos, Actions, [], Output) ->
    modify_node(Bt, nil, Actions, Output);
modify_kp_node(Bt, Nodes, Pos, [], NewNode, Output) ->
    {ok, ?rev2(NewNode, rest_nodes(Nodes, Pos, ?ts(Nodes), [])), Output, Bt};
modify_kp_node(Bt, Nodes, Pos, Actions, NewNode, Output) ->
    [{_, ActKey, _} | _]= Actions,
    NumNodes = ?ts(Nodes),
    {KpPos, KpInfo} = find_kp_pos(Bt, Nodes, Pos, NumNodes, ActKey),
    case KpPos == NumNodes of
        true  ->
            {KP, Output2} = modify_node(Bt, KpInfo, Actions, Output),
            NewNode2 = ?rev2(NewNode, rest_nodes(Nodes, Pos, NumNodes-1, [KP])),
            {NewNode2, Output2};
        false ->
            SplitFun = fun({_AType, AKey, _AValue}) ->
                not ?less(Bt, KpInfo#info.key, AKey)
            end,
            {LTEActs, GTActs} = lists:splitwith(SplitFun, Actions),
            {KP, Output2} = modify_node(Bt, KpInfo, LTEActs, Output),
            NewNode2 = ?rev2([KP], rest_rev_nodes(Nodes, Pos, KpPos-1, NewNode)),
            modify_kp_node(Bt, Nodes, KpPos + 1, GTActs, NewNode2, Output2)
    end.

modify_kv_node(_Bt, Nodes, Pos, [], NewNode, Output) ->
    {?rev2(NewNode, rest_nodes(Nodes, Pos, ?ts(Nodes), [])), Output};
modify_kv_node(Bt, Nodes, Pos, Actions, NewNode, Output) when Pos > ?ts(Nodes)->
    [{ActType, ActKey, ActValue} | RestActs] = Actions,
    case ActType of
        insert ->
            NewNode2 = [{ActKey, ActValue} | NewNode],
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode2, Output);
        remove ->
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode, Output);
        lookup ->
            Output2 = [{not_found, {ActKey, nil}} | Output],
            modify_kv_node(Bt, Nodes, Pos, RestActs, NewNode, Output2)
    end;
modify_kv_node(Bt, Nodes, Pos, Actions, NewNode, Output) ->
    [{ActType, ActKey, ActValue} | RestActs] = Actions,
    {KvPos, {Key, Value}} = find_kv_pos(Bt, Nodes, Pos, ?ts(Nodes), ActKey),
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
            NewNode3 = [element(KvPos, Nodes) | NewNode2],
            modify_kv_node(Bt, Nodes, KvPos + 1, Actions, NewNode3, Output)
    end.


rebalance_nodes(_Bt, [], Acc) ->
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
rebalance_nodes(Bt, [LInfo0 | RestL], [RInfo0 | RestR]) ->
    % Delay loading infos as long as possible.
    LNeeds = case LInfo0 of
        _ when is_integer(LInfo0) -> false;
        _ -> needs_balance(Bt, LInfo0)
    end,
    RNeeds = case RInfo0 of
        _ when is_integer(RInfo0) -> false;
        _ -> needs_balance(Bt, RInfo0)
    end,
    NewInfos = case LNeeds or RNeeds of
        false ->
            % Account for ?rev below.
            [RInfo0, LInfo0];
        true ->
            LInfo = get_info(Bt, LInfo0),
            RInfo = get_info(Bt, RInfo0),
            {ok, {T, L}} = couch_file:pread_term(Bt#btree.fd, LInfo#info.pos),
            {ok, {T, R}} = couch_file:pread_term(Bt#btree.fd, RInfo#info.pos),
            Num = LInfo#info.num + RInfo#info.num,
            % Notice that its R ++ L to account for the nodes
            % in R being < nodes in L
            split_nodes(Bt, T, Num, R ++ L)
    end,
    rebalance_nodes(Bt, RestL, ?rev2(NewInfos, RestR)).


split_nodes(Bt, Type, Num, Nodes) when Num =< ?MAX_SZ ->
    {ok, Pos} = couch_file:append_term(Bt#btree.fd, {Type, Nodes}),
    Key = case couch_file:pread_term(Bt#btree.fd, lists:last(Nodes)) of
        {ok, {K, _}} -> K;
        {ok, {_, K, _, _, _}} -> K
    end,
    [#info{type=Type, key=Key, pos=Pos, num=Num, red=undefined}];
split_nodes(Bt, Type, Num, Nodes) ->
    {Left, Right} = lists:split(Num div 2, Nodes),
    LInfos = split_nodes(Bt, Type, length(Left), Left),
    RInfos = split_nodes(Bt, Type, length(Right), Right),
    LInfos ++ RInfos.
    

needs_balance(_Bt, Info) ->
    Info#info.num =< ?MIN_SZ orelse Info#info.num >= ?MAX_SZ.


get_info(_Bt, nil) ->
    nil;
get_info(_Bt, #info{}=Info) ->
    Info;
get_info(Bt, Pos) when is_integer(Pos) ->
    {ok, {Type, Key, Pos2, Num, Red}} = couch_file:pread_term(Bt#btree.fd, Pos),
    #info{type=Type, key=Key, pos=Pos2, num=Num, red=Red}.


write_info(Bt, #info{type=Type, key=Key, pos=Pos, num=Num, red=undefined}) ->
    {ok, {Type, Nodes}} = couch_file:pread_term(Bt#btree.fd, Pos),
    Red = case Type of
        p -> reduce_kp_node(Bt, Nodes, []);
        v -> reduce_kv_node(Bt, Nodes, [], [])
    end,
    couch_file:append_term(Bt#btree.fd, {Type, Key, Pos, Num, Red}).


reduce_kp_node(#btree{reduce=nil}, _Nodes, _Reds) ->
    [];
reduce_kp_node(Bt, [], Reds) ->
    final_reduce(Bt, {[], Reds});
reduce_kp_node(Bt, NodeList, Reds) when length(Reds) >= 64 ->
    Red = final_reduce(Bt, {[], Reds}),
    reduce_kp_node(Bt, NodeList, [Red]);
reduce_kp_node(Bt, [Info | Rest], Reds) ->
    Info1 = get_info(Bt, Info),
    reduce_kp_node(Bt, Rest, [Info1#info.red | Reds]).


reduce_kv_node(#btree{reduce=nil}, _Nodes, _KVs, _Reds) ->
    [];
reduce_kv_node(Bt, [], KVs, Reds) ->
    final_reduce(Bt, {KVs, Reds});
reduce_kv_node(Bt, NodeList, KVs, Reds) when length(KVs) >= 64 ->
    Red = final_reduce(Bt, {KVs, []}),
    reduce_kv_node(Bt, NodeList, [], [Red | Reds]);
reduce_kv_node(Bt, [{K, V} | Rest], KVs, Reds) ->
    reduce_kv_node(Bt, Rest, [?assemble(Bt, K, V) | KVs], Reds);
reduce_kv_node(Bt, [Pos | Rest], KVs, Reds) ->
    {ok, {K, V}} = couch_file:pread_term(Bt#btree.fd, Pos),
    reduce_kv_node(Bt, Rest, [?assemble(Bt, K, V) | KVs], Reds).


%% @doc Read a node from disk.
get_node(#btree{fd = Fd}, NodePos) ->
    {ok, {NodeType, NodeList}} = couch_file:pread_term(Fd, NodePos),
    {NodeType, NodeList}.


%% @doc Write a btree node to disk.
write_node(p, Bt, NodeList0) ->
    NodeList = rebalance_nodes(Bt, NodeList0, []),
    {LastKey, Num, Infos} = flush_kp_node(Bt, NodeList, nil, 0, []),
    {ok, Pos} = couch_file:append_term(Bt#btree.fd, {p, Infos}),
    #info{type=p, key=LastKey, pos=Pos, num=Num};
write_node(v, Bt, NodeList) ->
    {LastKey, Num, Infos} = flush_kv_node(Bt, NodeList, nil, 0, []),
    {ok, Pos} = couch_file:append_term(Bt#btree.fd, {v, Infos}),
    #info{type=v, key=LastKey, pos=Pos, num=Num}.


flush_kp_node(Bt, [], Info, Num, Acc) ->
    Info1 = get_info(Bt, Info),
    {Info1#info.key, Num, ?rev(Acc)};
flush_kp_node(Bt, [#info{}=Info | Rest], _Last, Num, Acc) ->
    {ok, Pos} = write_info(Bt, Info),
    flush_kp_node(Bt, Rest, Info, Num + 1, [Pos | Acc]);
flush_kp_node(Bt, [Pos | Rest], _Last, Num, Acc) when is_integer(Pos) ->
    flush_kp_node(Bt, Rest, Pos, Num + 1, [Pos | Acc]).


flush_kv_node(_Bt, [], {K, _}, Num, Acc) ->
    {K, Num, ?rev(Acc)};
flush_kv_node(Bt, [], Pos, Num, Acc) when is_integer(Pos) ->
    {ok, {K, _}} = couch_file:pread_term(Bt#btree.fd, Pos),
    {K, Num, ?rev(Acc)};
flush_kv_node(Bt, [{K, V} | Rest], _Last, Num, Acc) ->
    {ok, Pos} = couch_file:append_term(Bt#btree.fd, {K, V}),
    flush_kv_node(Bt, Rest, {K, V}, Num + 1, [Pos | Acc]);
flush_kv_node(Bt, [Pos | Rest], _Last, Num, Acc) when is_integer(Pos) ->
    flush_kv_node(Bt, Rest, Pos, Num + 1, [Pos | Acc]).


complete_root(Bt, #info{type=p}=Info) ->
    write_info(Bt, Info);
complete_root(Bt, #info{num=Num}=Info) when Num =< ?MAX_SZ ->
    write_info(Bt, Info);
complete_root(Bt, #info{type=Type, pos=Pos, num=Num}) ->
    {v, Nodes} = get_node(Bt, Pos),
    Infos = split_nodes(Bt, Type, Num, Nodes),
    write_info(Bt, write_node(p, Bt, Infos)).


%% @doc Prioritize modification actions
%%
%% This is used to sort operations on a given key when using the
%% query_modify function. This ordering allows us to do things
%% like, "get current value and delete from tree".
op_order(fetch) -> 1;
op_order(remove) -> 2;
op_order(insert) -> 3.


%% @doc Compare two keys for ordering
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


%% @doc Find the node for a given key.
%%
%% This does a binary search over the Nodes tuple to find
%% the node where a given would be located.
find_kp_pos(Bt, Nodes, Start, End, Key) ->
    Load = fun(Pos) ->
        Info = get_info(Bt, Pos),
        Info#info.key
    end,
    Pos = find_key_pos(Bt, Load, Nodes, Start, End, Key),
    {Pos, get_info(Bt, element(Pos, Nodes))}.


find_kv_pos(Bt, Nodes, Start, End, Key) ->
    Load = fun(Pos) ->
        {ok, {K, _}} = couch_file:pread_term(Bt#btree.fd, Pos),
        K
    end,
    Pos = find_key_pos(Bt, Load, Nodes, Start, End, Key),
    {ok, {K, V}} = couch_file:pread_term(Bt#btree.fd, element(Pos, Nodes)),
    {Pos, {K, V}}.


find_key_pos(_Bt, _Load, _Nodes, Start, End, _Key) when Start == End ->
    End;
find_key_pos(Bt, Load, Nodes, Start, End, Key) ->
    Mid = Start + ((End - Start) div 2),
    MKey = Load(element(Mid, Nodes)),
    case ?less(Bt, MKey, Key) of
        true -> find_key_pos(Bt, Load, Nodes, Mid+1, End, Key);
        false -> find_key_pos(Bt, Load, Nodes, Start, Mid, Key)
    end.


%% @doc Return the rest of the nodes in a tuple given
%% the start and end positions. Tail is appended to
%% the result.
rest_nodes(Nodes, Start, End, Tail) ->
    rest_nodes(Nodes, Start, End, [], Tail).

%% @private
rest_nodes(_Nodes, Start, End, Acc, Tail) when Start > End ->
    ?rev2(Acc, Tail);
rest_nodes(Nodes, Start, End, Acc, Tail) ->
    rest_nodes(Nodes, Start + 1, End, [element(Start, Nodes) | Acc], Tail).


%% @doc Return the rest of the nodes in a tuple given
%% the start and end positions. Returns the results
%% in reversed order from how they appear in the Nodes tuple.
rest_rev_nodes(_Nodes, Start, End, Tail) when Start > End ->
    Tail;
rest_rev_nodes(Nodes, Start, End, Tail) ->
    rest_rev_nodes(Nodes, Start + 1, End, [element(Start, Nodes) | Tail]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Assertions for a valid b+tree %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% @doc Check that a btree is well formed.
well_formed(#btree{root=nil}) ->
    true;
well_formed(#btree{root=Root}=Bt) ->
    RootInfo = get_info(Bt, Root),
    well_formed_info(RootInfo, true),
    {Type, Nodes} = get_node(Bt, RootInfo#info.pos),
    well_formed_node(RootInfo, Type, Nodes),
    well_formed_children(Bt, RootInfo, Nodes, nil),
    true.

well_formed_info(Info, IsRoot) ->
    case Info#info.type of
        p -> ok;
        v -> ok;
        _ -> erlang:error({invalid_type, Info})
    end,
    case Info#info.pos of
        Pos when is_integer(Pos) andalso Pos >= 0 -> ok;
        Pos -> erlang:error({invalid_position, Info, Pos})
    end,
    case Info#info.num < ?MIN_SZ of
        true when IsRoot -> ok;
        true -> erlang:error({too_few_children, Info});
        _ -> ok
    end,
    case Info#info.num > ?MAX_SZ of
        true -> erlang:error({too_many_children, Info});
        _ -> ok
    end,
    case Info#info.red of
        undefined -> erlang:error({invalid_reductions, Info});
        _ -> ok
    end.

well_formed_node(Info, Type, Nodes) ->
    case Info#info.type of
        Type -> ok;
        _ -> erlang:error({type_mismatch, Info, Type})
    end,
    Num = length(Nodes),
    case Info#info.num of
        Num -> ok;
        _ -> erlang:error({invalid_num, Info, length(Nodes)})
    end.

well_formed_children(Bt, _, [], _) ->
    ok;
well_formed_children(Bt, #info{type=p}=Info, [Pos | Rest], Prev) ->
    case Pos of
        _ when is_integer(Pos) -> ok;
        _ -> erlang:error({invalid_child_pos, Info, Pos})
    end,
    case Pos < Info#info.pos of
        true -> ok;
        _ -> erlang:error({invalid_child_after_info, Info, Pos})
    end,
    CInfo = get_info(Bt, Pos),
    case is_record(CInfo, info) of
        true -> ok;
        _ -> erlang:error({invalid_info, Info, Pos})
    end,
    case CInfo#info.key =< Info#info.key of
        true -> ok;
        _ -> erlang:error({invalid_child_key, Info, CInfo})
    end,
    case CInfo#info.pos < Info#info.pos of
        true -> ok;
        _ -> erlang:error({invalid_child_pos, Info, CInfo})
    end,
    well_formed_info(CInfo, false),
    {Type, Nodes} = get_node(Bt, CInfo#info.pos),
    well_formed_node(CInfo, Type, Nodes),
    well_formed_children(Bt, CInfo, Nodes, nil),
    case Prev of
        nil -> ok;
        _ when CInfo#info.key > Prev#info.key -> ok;
        _ -> erlang:error({invalid_key_order, CInfo, Prev})
    end,
    well_formed_children(Bt, Info, Rest, CInfo);
well_formed_children(Bt, Info, [Pos | Rest], Prev) ->
    case Pos of
        _ when is_integer(Pos) andalso Pos >= 0 -> ok;
        _ -> erlang:error({invalid_kv_pos, Info, Pos})
    end,
    case Pos < Info#info.pos of
        true -> ok;
        _ -> erlang:error({invalid_kv_pos2, Info, Pos})
    end,
    {ok, {Key, _}} = couch_file:pread_term(Bt#btree.fd, Pos),
    case Key =< Info#info.key of
        true -> ok;
        _ -> erlang:error({invalid_kv_key, Info, Key})
    end,
    case Prev of
        nil -> ok;
        _ when Key > Prev -> ok;
        _ -> erlang:error({invalid_kv_order, Info, Key, Prev})
    end,
    well_formed_children(Bt, Info, Rest, Key).
