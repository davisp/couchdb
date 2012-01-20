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

%% @doc Data structure used to represent document edit histories.

%% A key tree is used to represent the edit history of a document. Each node of
%% the tree represents a particular version. Relations between nodes represent
%% the order that these edits were applied. For instance, a set of three edits
%% would produce a tree of versions A->B->C indicating that edit C was based on
%% version B which was in turn based on A. In a world without replication (and
%% no ability to disable MVCC checks), all histories would be forced to be
%% linear lists of edits due to constraints imposed by MVCC (ie, new edits must
%% be based on the current version). However, we have replication, so we must
%% deal with not so easy cases, which lead to trees.
%%
%% Consider a document in state A. This doc is replicated to a second node. We
%% then edit the document on each node leaving it in two different states, B
%% and C. We now have two key trees, A->B and A->C. When we go to replicate a
%% second time, the key tree must combine these two trees which gives us
%% A->(B|C). This is how conflicts are introduced. In terms of the key tree, we
%% say that we have two leaves (B and C) that are not deleted. The presense of
%% the multiple leaves indicate conflict. To remove a conflict, one of the
%% edits (B or C) can be deleted, which results in, A->(B|C->D) where D is an
%% edit that is specially marked with the a deleted=true flag.
%%
%% What makes this a bit more complicated is that there is a limit to the
%% number of revisions kept, specified in couch_db.hrl (default is 1000). When
%% this limit is exceeded only the last 1000 are kept. This comes in to play
%% when branches are merged. The comparison has to begin at the same place in
%% the branches. A revision id is of the form N-XXXXXXX where N is the current
%% revision. So each path will have a start number, calculated in
%% couch_doc:to_path using the formula N - length(RevIds) + 1 So, .eg. if a doc
%% was edit 1003 times this start number would be 4, indicating that 3
%% revisions were truncated.
%%
%% This comes into play in @see merge_at/3 which recursively walks down one
%% tree or the other until they begin at the same revision.

-module(couch_key_tree).

-export([merge/3, find_missing/2, get_key_leafs/2, get_full_key_paths/2, get/2]).
-export([get_all_leafs/1, count_leafs/1, remove_leafs/2, get_all_leafs_full/1, stem/2]).
-export([map/2, mapfold/3, map_leafs/2, fold/3]).

-include("couch_db.hrl").

%% @doc Merge a path with a list of paths and stem to the given length.
merge(Paths, Path, Depth) ->
    {Merged, Conflicts} = merge(Paths, Path),
    {stem(Merged, Depth), Conflicts}.

%% @doc Merge a path with an existing list of paths, returning a new list of
%% paths. A return of conflicts indicates a new conflict was discovered in this
%% merge. Conflicts may already exist in the original list of paths.
merge(Paths, Path) ->
    {Merged, Status} = merge_one(Paths, Path, []),
    {lists:sort(Merged), Status}.

merge_one([], Path, []) ->
    % Special case when the doc is created and has
    % no revisions to merge with
    {[Path], new_leaf};
merge_one([], Path, Acc) ->
    % Merge created a new top-level branch
    {[Path | Acc], new_branch};
merge_one([{Start, Tree} | Rest], {PathStart, PathVals}, Acc) ->
    try
        {[Merged], Status} = merge_at([Tree], PathStart - Start, [PathVals]),
        MergedStart = lists:min([Start, PathStart]),
        {Rest ++ [{MergedStart, Merged} | Acc], Status}
    catch throw:no_merge ->
        merge_one(Rest, {PathStart, PathVals}, [{Start, Tree} | Acc])
    end.

merge_at([], _Depth, _Path) ->
    throw(no_merge);
merge_at([{Key, Value, SubTree} | Sibs], Depth, Path) when Depth > 0 ->
    % inserted starts later than committed,
    % need to drill into committed subtree
    try
        {Merged0, Status0} = merge_at(SubTree, Depth - 1, Path),
        {[{Key, Value, Merged0} | Sibs], Status0}
    catch throw:no_merge ->
        % first branch didn't merge, move to next branch
        {Merged1, Status1} = merge_at(Sibs, Depth, Path),
        {[{Key, Value, SubTree} | Merged1], Status1}
    end;
merge_at(OurTree, Depth, [{Key, Value, SubPath}]) when Depth < 0 ->
    % inserted starts earlier than committed,
    % need to drill into insert subtree
    {Merged, Status} = merge_at(OurTree, Depth + 1, SubPath),
    {[{Key, Value, Merged}], Status};
merge_at([{Key, V1, SubTree} | Sibs], 0, [{Key, V2, SubPath}]) ->
    {Merged, Status} = merge_simple(SubTree, SubPath),
    {[{Key, value_pref(V1, V2), Merged} | Sibs], Status};
merge_at([{OurKey, _, _} | _], 0, [{Key, _, _}]) when OurKey > Key ->
    % siblings keys are ordered, no point in continuing
    throw(no_merge);
merge_at([Tree | Sibs], 0, InsertTree) ->
    {Merged, Status} = merge_at(Sibs, 0, InsertTree),
    {[Tree | Merged], Status}.

merge_simple([], B) ->
    % Inserted path extends a leaf
    {B, new_leaf};
merge_simple(A, []) ->
    % Ran out of insertion path at an internal node
    {A, internal_node};
merge_simple([{Key, V1, SubTree} | NextTree], [{Key, V2, SubPath}]) ->
    % Keys match, continue descending along this branch
    {Merged, Status} = merge_simple(SubTree, SubPath),
    {[{Key, value_pref(V1, V2), Merged} | NextTree], Status};
merge_simple([{A, _, _} = Tree | Siblings], [{B, _, _}] = Path) when A < B ->
    % Keep trying siblings until we run out or find a
    % key A > B
    {Merged, Status} = merge_simple(Siblings, Path),
    {[Tree | Merged], Status};
merge_simple(Tree, [Path]) ->
    % Sorted keys means we know the rest of the path
    % is a new branch
    {[Path | Tree], new_branch}.

find_missing(_Tree, []) ->
    [];
find_missing([], SeachKeys) ->
    SeachKeys;
find_missing([{Start, {Key, Value, SubTree}} | RestTree], SeachKeys) ->
    PossibleKeys = [{KeyPos, KeyValue} || {KeyPos, KeyValue} <- SeachKeys, KeyPos >= Start],
    ImpossibleKeys = [{KeyPos, KeyValue} || {KeyPos, KeyValue} <- SeachKeys, KeyPos < Start],
    Missing = find_missing_simple(Start, [{Key, Value, SubTree}], PossibleKeys),
    find_missing(RestTree, ImpossibleKeys ++ Missing).

find_missing_simple(_Pos, _Tree, []) ->
    [];
find_missing_simple(_Pos, [], SeachKeys) ->
    SeachKeys;
find_missing_simple(Pos, [{Key, _, SubTree} | RestTree], SeachKeys) ->
    PossibleKeys = [{KeyPos, KeyValue} || {KeyPos, KeyValue} <- SeachKeys, KeyPos >= Pos],
    ImpossibleKeys = [{KeyPos, KeyValue} || {KeyPos, KeyValue} <- SeachKeys, KeyPos < Pos],

    SrcKeys2 = PossibleKeys -- [{Pos, Key}],
    SrcKeys3 = find_missing_simple(Pos + 1, SubTree, SrcKeys2),
    ImpossibleKeys ++ find_missing_simple(Pos, RestTree, SrcKeys3).


filter_leafs([], _Keys, FilteredAcc, RemovedKeysAcc) ->
    {FilteredAcc, RemovedKeysAcc};
filter_leafs([{Pos, [{LeafKey, _}|_]} = Path |Rest], Keys, FilteredAcc, RemovedKeysAcc) ->
    FilteredKeys = lists:delete({Pos, LeafKey}, Keys),
    if FilteredKeys == Keys ->
        % this leaf is not a key we are looking to remove
        filter_leafs(Rest, Keys, [Path | FilteredAcc], RemovedKeysAcc);
    true ->
        % this did match a key, remove both the node and the input key
        filter_leafs(Rest, FilteredKeys, FilteredAcc, [{Pos, LeafKey} | RemovedKeysAcc])
    end.

% Removes any branches from the tree whose leaf node(s) are in the Keys
remove_leafs(Trees, Keys) ->
    % flatten each branch in a tree into a tree path
    Paths = get_all_leafs_full(Trees),

    % filter out any that are in the keys list.
    {FilteredPaths, RemovedKeys} = filter_leafs(Paths, Keys, [], []),

    SortedPaths = lists:sort(
        [{Pos + 1 - length(Path), Path} || {Pos, Path} <- FilteredPaths]
    ),

    % convert paths back to trees
    NewTree = lists:foldl(
        fun({StartPos, Path},TreeAcc) ->
            [SingleTree] = lists:foldl(
                fun({K,V},NewTreeAcc) -> [{K,V,NewTreeAcc}] end, [], Path),
            {NewTrees, _} = merge(TreeAcc, {StartPos, SingleTree}),
            NewTrees
        end, [], SortedPaths),
    {NewTree, RemovedKeys}.


% get the leafs in the tree matching the keys. The matching key nodes can be
% leafs or an inner nodes. If an inner node, then the leafs for that node
% are returned.
get_key_leafs(Tree, Keys) ->
    get_key_leafs(Tree, Keys, []).

get_key_leafs(_, [], Acc) ->
    {Acc, []};
get_key_leafs([], Keys, Acc) ->
    {Acc, Keys};
get_key_leafs([{Pos, Tree}|Rest], Keys, Acc) ->
    {Gotten, RemainingKeys} = get_key_leafs_simple(Pos, [Tree], Keys, []),
    get_key_leafs(Rest, RemainingKeys, Gotten ++ Acc).

get_key_leafs_simple(_Pos, _Tree, [], _KeyPathAcc) ->
    {[], []};
get_key_leafs_simple(_Pos, [], KeysToGet, _KeyPathAcc) ->
    {[], KeysToGet};
get_key_leafs_simple(Pos, [{Key, _Value, SubTree}=Tree | RestTree], KeysToGet, KeyPathAcc) ->
    case lists:delete({Pos, Key}, KeysToGet) of
    KeysToGet -> % same list, key not found
        {LeafsFound, KeysToGet2} = get_key_leafs_simple(Pos + 1, SubTree, KeysToGet, [Key | KeyPathAcc]),
        {RestLeafsFound, KeysRemaining} = get_key_leafs_simple(Pos, RestTree, KeysToGet2, KeyPathAcc),
        {LeafsFound ++ RestLeafsFound, KeysRemaining};
    KeysToGet2 ->
        LeafsFound = get_all_leafs_simple(Pos, [Tree], KeyPathAcc),
        LeafKeysFound = [{LeafPos, LeafRev} || {_, {LeafPos, [LeafRev|_]}}
            <- LeafsFound],
        KeysToGet3 = KeysToGet2 -- LeafKeysFound,
        {RestLeafsFound, KeysRemaining} = get_key_leafs_simple(Pos, RestTree, KeysToGet3, KeyPathAcc),
        {LeafsFound ++ RestLeafsFound, KeysRemaining}
    end.

get(Tree, KeysToGet) ->
    {KeyPaths, KeysNotFound} = get_full_key_paths(Tree, KeysToGet),
    FixedResults = [ {Value, {Pos, [Key0 || {Key0, _} <- Path]}} || {Pos, [{_Key, Value}|_]=Path} <- KeyPaths],
    {FixedResults, KeysNotFound}.

get_full_key_paths(Tree, Keys) ->
    get_full_key_paths(Tree, Keys, []).

get_full_key_paths(_, [], Acc) ->
    {Acc, []};
get_full_key_paths([], Keys, Acc) ->
    {Acc, Keys};
get_full_key_paths([{Pos, Tree}|Rest], Keys, Acc) ->
    {Gotten, RemainingKeys} = get_full_key_paths(Pos, [Tree], Keys, []),
    get_full_key_paths(Rest, RemainingKeys, Gotten ++ Acc).


get_full_key_paths(_Pos, _Tree, [], _KeyPathAcc) ->
    {[], []};
get_full_key_paths(_Pos, [], KeysToGet, _KeyPathAcc) ->
    {[], KeysToGet};
get_full_key_paths(Pos, [{KeyId, Value, SubTree} | RestTree], KeysToGet, KeyPathAcc) ->
    KeysToGet2 = KeysToGet -- [{Pos, KeyId}],
    CurrentNodeResult =
    case length(KeysToGet2) =:= length(KeysToGet) of
    true -> % not in the key list.
        [];
    false -> % this node is the key list. return it
        [{Pos, [{KeyId, Value} | KeyPathAcc]}]
    end,
    {KeysGotten, KeysRemaining} = get_full_key_paths(Pos + 1, SubTree, KeysToGet2, [{KeyId, Value} | KeyPathAcc]),
    {KeysGotten2, KeysRemaining2} = get_full_key_paths(Pos, RestTree, KeysRemaining, KeyPathAcc),
    {CurrentNodeResult ++ KeysGotten ++ KeysGotten2, KeysRemaining2}.

get_all_leafs_full(Tree) ->
    get_all_leafs_full(Tree, []).

get_all_leafs_full([], Acc) ->
    Acc;
get_all_leafs_full([{Pos, Tree} | Rest], Acc) ->
    get_all_leafs_full(Rest, get_all_leafs_full_simple(Pos, [Tree], []) ++ Acc).

get_all_leafs_full_simple(_Pos, [], _KeyPathAcc) ->
    [];
get_all_leafs_full_simple(Pos, [{KeyId, Value, []} | RestTree], KeyPathAcc) ->
    [{Pos, [{KeyId, Value} | KeyPathAcc]} | get_all_leafs_full_simple(Pos, RestTree, KeyPathAcc)];
get_all_leafs_full_simple(Pos, [{KeyId, Value, SubTree} | RestTree], KeyPathAcc) ->
    get_all_leafs_full_simple(Pos + 1, SubTree, [{KeyId, Value} | KeyPathAcc]) ++ get_all_leafs_full_simple(Pos, RestTree, KeyPathAcc).

get_all_leafs(Trees) ->
    get_all_leafs(Trees, []).

get_all_leafs([], Acc) ->
    Acc;
get_all_leafs([{Pos, Tree}|Rest], Acc) ->
    get_all_leafs(Rest, get_all_leafs_simple(Pos, [Tree], []) ++ Acc).

get_all_leafs_simple(_Pos, [], _KeyPathAcc) ->
    [];
get_all_leafs_simple(Pos, [{KeyId, Value, []} | RestTree], KeyPathAcc) ->
    [{Value, {Pos, [KeyId | KeyPathAcc]}} | get_all_leafs_simple(Pos, RestTree, KeyPathAcc)];
get_all_leafs_simple(Pos, [{KeyId, _Value, SubTree} | RestTree], KeyPathAcc) ->
    get_all_leafs_simple(Pos + 1, SubTree, [KeyId | KeyPathAcc]) ++ get_all_leafs_simple(Pos, RestTree, KeyPathAcc).


count_leafs([]) ->
    0;
count_leafs([{_Pos,Tree}|Rest]) ->
    count_leafs_simple([Tree]) + count_leafs(Rest).

count_leafs_simple([]) ->
    0;
count_leafs_simple([{_Key, _Value, []} | RestTree]) ->
    1 + count_leafs_simple(RestTree);
count_leafs_simple([{_Key, _Value, SubTree} | RestTree]) ->
    count_leafs_simple(SubTree) + count_leafs_simple(RestTree).


fold(_Fun, Acc, []) ->
    Acc;
fold(Fun, Acc0, [{Pos, Tree}|Rest]) ->
    Acc1 = fold_simple(Fun, Acc0, Pos, [Tree]),
    fold(Fun, Acc1, Rest).

fold_simple(_Fun, Acc, _Pos, []) ->
    Acc;
fold_simple(Fun, Acc0, Pos, [{Key, Value, SubTree} | RestTree]) ->
    Type = if SubTree == [] -> leaf; true -> branch end,
    Acc1 = Fun({Pos, Key}, Value, Type, Acc0),
    Acc2 = fold_simple(Fun, Acc1, Pos+1, SubTree),
    fold_simple(Fun, Acc2, Pos, RestTree).


map(_Fun, []) ->
    [];
map(Fun, [{Pos, Tree}|Rest]) ->
    case erlang:fun_info(Fun, arity) of
    {arity, 2} ->
        [NewTree] = map_simple(fun(A,B,_C) -> Fun(A,B) end, Pos, [Tree]),
        [{Pos, NewTree} | map(Fun, Rest)];
    {arity, 3} ->
        [NewTree] = map_simple(Fun, Pos, [Tree]),
        [{Pos, NewTree} | map(Fun, Rest)]
    end.

map_simple(_Fun, _Pos, []) ->
    [];
map_simple(Fun, Pos, [{Key, Value, SubTree} | RestTree]) ->
    Value2 = Fun({Pos, Key}, Value,
            if SubTree == [] -> leaf; true -> branch end),
    [{Key, Value2, map_simple(Fun, Pos + 1, SubTree)} | map_simple(Fun, Pos, RestTree)].


mapfold(_Fun, Acc, []) ->
    {[], Acc};
mapfold(Fun, Acc, [{Pos, Tree} | Rest]) ->
    {[NewTree], Acc2} = mapfold_simple(Fun, Acc, Pos, [Tree]),
    {Rest2, Acc3} = mapfold(Fun, Acc2, Rest),
    {[{Pos, NewTree} | Rest2], Acc3}.

mapfold_simple(_Fun, Acc, _Pos, []) ->
    {[], Acc};
mapfold_simple(Fun, Acc, Pos, [{Key, Value, SubTree} | RestTree]) ->
    {Value2, Acc2} = Fun({Pos, Key}, Value,
            if SubTree == [] -> leaf; true -> branch end, Acc),
    {SubTree2, Acc3} = mapfold_simple(Fun, Acc2, Pos + 1, SubTree),
    {RestTree2, Acc4} = mapfold_simple(Fun, Acc3, Pos, RestTree),
    {[{Key, Value2, SubTree2} | RestTree2], Acc4}.


map_leafs(_Fun, []) ->
    [];
map_leafs(Fun, [{Pos, Tree}|Rest]) ->
    [NewTree] = map_leafs_simple(Fun, Pos, [Tree]),
    [{Pos, NewTree} | map_leafs(Fun, Rest)].

map_leafs_simple(_Fun, _Pos, []) ->
    [];
map_leafs_simple(Fun, Pos, [{Key, Value, []} | RestTree]) ->
    Value2 = Fun({Pos, Key}, Value),
    [{Key, Value2, []} | map_leafs_simple(Fun, Pos, RestTree)];
map_leafs_simple(Fun, Pos, [{Key, Value, SubTree} | RestTree]) ->
    [{Key, Value, map_leafs_simple(Fun, Pos + 1, SubTree)} | map_leafs_simple(Fun, Pos, RestTree)].


stem(Trees, Limit) ->
    % flatten each branch in a tree into a tree path, sort by starting rev #
    Paths = lists:sort(lists:map(fun({Pos, Path}) ->
        StemmedPath = lists:sublist(Path, Limit),
        {Pos + 1 - length(StemmedPath), StemmedPath}
    end, get_all_leafs_full(Trees))),

    % convert paths back to trees
    lists:foldl(
        fun({StartPos, Path},TreeAcc) ->
            [SingleTree] = lists:foldl(
                fun({K,V},NewTreeAcc) -> [{K,V,NewTreeAcc}] end, [], Path),
            {NewTrees, _} = merge(TreeAcc, {StartPos, SingleTree}),
            NewTrees
        end, [], Paths).


value_pref(Tuple, _) when is_tuple(Tuple),
        (tuple_size(Tuple) == 3 orelse tuple_size(Tuple) == 4) ->
    Tuple;
value_pref(_, Tuple) when is_tuple(Tuple),
        (tuple_size(Tuple) == 3 orelse tuple_size(Tuple) == 4) ->
    Tuple;
value_pref(?REV_MISSING, Other) ->
    Other;
value_pref(Other, ?REV_MISSING) ->
    Other;
value_pref(Last, _) ->
    Last.


% Tests moved to test/etap/06?-*.t

