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
%% say that we have two leaves (B and C) that are not deleted. The presene of
%% the multiple leaves indicate conflict. To remove a conflict, one of the
%% edits (B or C) can be deleted, which results in, A->(B|C->D) where D is an
%% edit that is specially marked with the a deleted=true flag.
%%
%% What makes this a bit more complicated is that there is a limit to the
%% number of revisions kept, specified in couch_db.hrl (default is 1000). When
%% this limit is exceeded only the last 1000 are kept. This comes in to play
%% when branches are merged. The comparison has to begin at the same place in
%% the branches. A revision id is of the form N-XXXXXXX where N is the current
%% revision depth. So each path will have a start number, calculated in
%% couch_doc:to_path using the formula N - length(RevIds) + 1 So, .eg. if a doc
%% was edit 1003 times this start number would be 4, indicating that 3
%% revisions were truncated.
%%
%% This comes into play in @see merge_at/3 which recursively walks down one
%% tree or the other until they begin at the same revision.

-module(couch_key_tree).

-export([merge/3, stem/2, delete/2]).

-export([find_missing/2, get_key_leafs/2, get_full_key_paths/2, get/2]).
-export([get_all_leafs/1, count_leafs/1, get_all_leafs_full/1]).
-export([map/2, mapfold/3, map_leafs/2]).

-include("couch_db.hrl").

% Node::term() is really a node(), but we don't want to require R13B04 yet
-type node() :: {Key::term(), Value::term(), [Node::term()]}.
-type tree() :: {Depth::pos_integer(), [node()]}.
-type revtree() :: [tree()].
-type path() :: {Value::term(), {Depth:pos_integer(), [term()]}}.


%% Testing ideas:
%% Merge:
%%   Introduce a new branch with a deleted leaf.
%%   Collapse two branches by connecting stemmed branches with path.
%%   Only has conflicts if one is introduced?
%%



%% @doc Merge a path into the given tree and then stem the result.
%% Although Tree is of type tree(), it must not contain any branches.
-spec merge(revtree(), tree(), pos_integer()) ->
                {revtree(), conflicts | no_conflicts}.
merge(RevTree, Tree, StemDepth) ->
    {Merged, Conflicts} = merge(RevTree, Tree),
    {stem(Merged, StemDepth), Conflicts}.


%% @doc Merge a path into a tree.
%% XXX: Is the rest of this comment true?
%% A return of conflicts indicates a new conflict was discovered in this
%% merge. Conflicts may already exist in the original list of paths.
-spec merge(revtree(), tree()) -> {revtree(), conflicts | no_conflicts}.
merge(RevTree, Tree) ->
    {Merged, Conflicts} = merge_tree(RevTree, Tree, []),
    case Conflicts of
        true -> {lists:sort(Merged), conflicts};
        false -> {lists:sort(Merged), no_conflicts}
    end.


%% @private
%% @doc Attempt to merge Tree into each branch of the RevTree.
%% If it can't find a branch that the new tree merges into, add it as a
%% new branch in the RevTree.
-spec merge_tree(revtree(), tree(), revtree()) -> {revtree(), boolean()}.
merge_tree([], Tree, []) -> 
    {[Tree], false};
merge_tree([], Tree, Merged) ->
    {[Tree | Merged], true};
merge_tree([{Depth, Nodes} | Rest], {IDepth, INodes}=Tree, Merged) ->
    % For the intrepid observer following along at home, notice what we're
    % doing here with (Depth - IDepth). This tells us which of the two
    % branches (Nodes or INodes) we need to seek into. If Depth > IDepth
    % that means we need go into INodes to find where we line up with
    % Nodes. If Depth < IDepth, its obviously the other way. If it turns
    % out that (Depth - IDepth) == 0, then we know that this is where
    % we begin our actual merge operation (ie, looking for key matches).
    % Its helpful to note that this whole moving into sub-branches is due
    % to how we store trees that have been stemmed. When a tree() is
    % stemmed so that the root node is lost, we wrap it in a tuple with
    % the number keys that have been droped. This number is the depth
    % value that's used through out this module.
    case merge_at(Nodes, Depth - IDepth, INodes) of
        {NewNodes, Conflicts} ->
            NewDepth = lists:min([Depth, InsDepth]),
            {Rest ++ [{NewDepth, NewTree} | Merged], Conflicts};
        unmerged ->
            merge(Rest, Tree, [{Depth, Nodes} | Merged])
    end.

%% @private
%% @doc Locate the point at which merging can start.
%% Because of stemming we may need to seek into one of the branches
%% before we can start comparing node keys. If one of the branches
%% ends up running out of nodes we know that these two branches can
%% not be merged.
-spec merge_at([node()], integer(), [node()]) -> {revtree(), boolean()} | fail.
merge_at(_Nodes, _Pos, []) ->
    fail;
merge_at([], _Pos, _INodes) ->
    fail;
merge_at(Nodes, Pos, [{_, _, [NextNode]} | Rest]) when Place > 0 ->
    % Depth was bigger than IDepth, so we need to discard from the
    % current branch to find where INodes might start matching.
    case merge_at(Nodes, Pos + 1, [NextTree]) of
        {Merged, Conflicts} -> {[{K, V, Merged}], Conflicts}
        fail -> fail
    end;
merge_at([{K, V, SubTree} | Sibs], Pos, INodes) when Place < 0 ->
    % When Pos is negative, Depth was less than IDepth, so we
    % need to discard from ITree
    case merge_at(SubTree, Pos - 1, INodes) of
        {Merged, Conflicts} ->
            {[{K, V, Merged} | Sibs], Conflicts};
        fail ->
            % Merging along the subtree failed. We need to also try
            % merging the insert branch against the siblings of this
            % node.
            case merge_at(Sibs, Pos, INodes) of
                {Merged, Conflicts} -> {[{K, V, SubTree} | Merged], Conflicts};
                fail -> fail
            end
    end;
merge_at([{K, V1, Nodes} | Sibs], 0, [{K, _, INodes}]) ->
    % Keys are equal. At this point we have found a possible starting
    % position for our merge to take place.
    {Merged, Conflicts} = merge_extend(Nodes, INodes),
    {[{K, V, Merged} | Sibs], Conflicts};
merge_at([{K1, _, _} | _], 0, [{K2, _, _}]) when K1 > K2 ->
    % Siblings keys are ordered, no point in continuing
    fail;
merge_at([Tree | Sibs], 0, INodes) ->
    % INodes key comes after this key, so move on to the next sibling.
    case merge_at(Sibs, 0, INodes) of
        {Merged, Conflicts} -> {[Tree | Merged], Conflicts};
        fail -> fail
    end.

-spec merge_extend(tree(), tree()) -> {tree(), boolean()}.
merge_extend([], B) ->
    % Insert branch simply extends this one, so the new branch
    % is exactly B
    {B, false};
merge_extend(A, []) ->
    % Insert branch ends an internal node in our original revtree()
    % so the end result is exactly our original revtree.
    {A, false};
merge_extend([{K, V, SubA} | NextA], [{K, _, SubB}]) ->
    % Here we're simply extending the path to the next deeper
    % level in the two branches.
    {Merged, Conflicts} = merge_extend(SubA, SubB),
    {[{Key, V, Merged} | NextA], Conflicts};
merge_extend([{K1, _, _}=NodeA | Rest], [{K2, _, _}=NodeB]) when K1 > K2 ->
    % Keys are ordered so we know this is where the insert branch needs
    % to be inserted into the tree. We also know that this creates a new
    % branch so we have a new leaf to report.
    {[NodeB, NodeA | Rest], true};
merge_extend([Tree | RestA], NextB) ->
    % Here we're moving on to the next sibling to try and extend our
    % merge even deeper. The orelse length check is due to the fact
    % that the key in NextB might be larger than the largest key in
    % RestA which means we've created a new branch which in turn means
    % we've created a new leaf.
    {Merged, Conflicts} = merge_extend(RestA, NextB),
    {[Tree | Merged], Conflicts orelse length(Merged) =/= length(NextA)}.


%% @doc Stem a tree to have paths no longer than Length.
%% This works by getting the path from each leaf up the tree towards
%% root as far as possible. These paths are then trimmed and then
%% merged back to form the branches of the revision tree.
-spec stem(revtree(), pos_integer()) -> revtree().
stem(RevTree, Length) ->
    % Create a list of {Depth, Path} pairs where Path has been
    % limited to Length nodes. LeafDepth is the depth of the leaf
    % but LeafPaths refers to the Depth of the first node in the path.
    % This is so we can sort them below to hopefully gurantee that
    % we don't return a revision tree that could be simplified any
    % further. See COUCHDB-1163 for why this is important.
    LeafPaths = lists:map(fun({LeafDepth, Path}) ->
        StemmedPath = lists:sublist(Path, StemDepth),
        {LeafDepth + 1 - length(StemmedPath), StemmedPath}
    end, get_all_leafs_full(RevTree)),
    
    % Convert our sorted list of paths back into a revision tree by
    % sorting them in order of closest to the root of the tree. After
    % sorting we're merely merging each new path into our accumulated
    % new revision tree.
    lists:foldl(fun({Depth, Path}, NewRevTreeAcc) ->
        [SingleRevTree] = lists:foldl(fun({K, V}, SingleRevTreeAcc) ->
            [{K, V, SingleRevTreeAcc}]
        end, [], Path),
        {NewRevTree, _} = merge(NewRevTreeAcc, {Depth, SingleRevTree}),
        NewRevTree
    end, [], lists:sort(LeafPaths)).


%% @doc Delete the nodes unique to branches identified in Leaves.
%% This works very similarly to stem/2 except that here instead
%% of limiting the length of each path we're removing any path
%% identified by a key value in Leaves. Once we remove these we
%% simple rebuild the revision tree as in stem/2 and return it
%% and the keys we removed.
-spec delete
delete(RevTree, Leaves) ->
    % Get the full path for each leaf value in the tree.
    Paths = get_all_leafs_full(RevTree),

    % Remove any paths that are identified by keys in Leaves

    FiltPred = fun({LDepth, [{LKey, _} | _]=Path}, {FiltLeaves, Filt, Rem}) ->
        case lists:delete({Depth, LKey}, FiltLeaves) of
            FiltLeaves ->
                % LKey not in Leaves so we keep it. Also convert our leaf
                % depth into a normal branch depth.
                {FiltLeaves, [{LDepth + 1 - length(Path), Path} | Filt], Rem};
            FiltLeaves2 ->
                % LKey was in FiltLeaves drop it and record that we found it.
                {FiltLeaves2, Filt, [{LDepth, LKey} | Rem]}
        end
    end,
    {_, Filtered, Removed} = lists:foldl(FiltPred, {Leaves, [], []}, Paths),

    % The same as we do in stem/2, convert the sorted paths back into
    % a revision tree though we also need to return the list of
    % {LDepth, LKey} pairs that we filtered from the tree.
    RetRevTree = lists:foldl(fun({Depth, Path}, NewRevTreeAcc) ->
        [SingleRevTree] = lists:foldl(fun({K, V}, SingleRevTreeAcc) ->
            [{K, V, SingleRevTreeAcc}]
        end, [], Path),
        {NewRevTree, _} = merge(NewRevTreeAcc, {Depth, SingleRevTree}),
        NewRevTree
    end, [], lists:sort(LeafPaths)),
    {RetRevTree, Removed}.


%% @doc Fold over the nodes in a revision tree.
%% This is a general function that is reused exctensively through the
%% rest of this file (eventually). It passes each key to a fun that
%% can return {ok, Acc}, {skip, Acc}, or {stop, Acc}. Returning
%% {skip, Acc} will abort continuing further down the current branch and
%% move on to the branch siblings.
foldl(_Fun, VAcc, []) ->
    {ok, VAcc};
foldl(Fun, VAcc0, [{Depth, Tree} | Rest]) ->
    case foldl(Fun, VAcc0, [], Depth, Tree) of
        {stop, VAcc1} ->
            {ok, VAcc1};
        {_, VAcc1} ->
            foldl(Fun, VAcc1, [], Depth, Rest)}
    end.

foldl(_Fun, VAcc, _PAcc, _Pos, []) ->
    {ok, VAcc};
foldl(Fun, VAcc0, PAcc0, Depth, [{Key, Value, SubTree} | RestTree]) ->
    NodeType = case SubTree of [] -> leaf; _ -> branch end,
    case Fun({Depth, Key, Value}, NodeType, VAcc0, PAcc0) of
        {ok, VAcc1, PAcc1} ->
            % Recurse and then continue with RestTree
            case foldl(Fun, VAcc1, PAcc1, Pos+1, SubTree) of
                {stop, VAcc2} ->
                    {stop, VAcc2};
                {_, VAcc2} ->
                    foldl(Fun, VAcc2, PAcc0, Pos, RestTree);
            end;
        {skip, _, VAcc1} ->
            foldl(Fun, VAcc2, PAcc0, Pos, RestTree);
        {stop, _, VAcc1} ->
            {stop, Acc1}
    end.


get_all_leafs_full(Tree) ->
    Pref = fun
        ({Depth, Key, Value}, branch, Acc, PAcc) ->
            {ok, Acc, [{Key, Val} | PAcc]};
        ({Depth, Key, Value}, leaf, Acc) ->
            Leaf = {Pos, [{Key, Val} | PAcc]},
            {ok, [Leaf | Acc], PAcc}
    end,
    foldl(RevTree, Pred, []).


get_all_leafs(RevTree) ->
    Pred = fun
        ({_Depth, Key, _Val}, branch, Acc, PAcc) ->
            {ok, Acc, [Key | PAcc]};
        ({Depth, Key, Val}, leaf, Acc, PAcc) ->
            Leaf = {Val, {Depth, [Key | PAcc]}},
            {ok, [Leaf | Acc], PAcc}
    end,
    foldl(RevTree, Pred, []).


count_leaves(RevTree) ->
    Pred = fun
        (_, leaf, Count, _) ->
            {ok, Count + 1, nil};
        (_, _, Count) ->
            {ok, Count, nil}
    end,
    foldl(RevTree, Pred, 0).


find_missing(RevTree, Keys0) ->
    Pred = fun({Depth, Key, _}, _, KAcc0, _) ->
        KAcc1 = sets:del_element(Key, KAcc0),
        case sets:size(KAcc1) of
            0 -> {stop, []};
            _ -> {ok, KAcc0, nil}
        end
    end,
    Keys1 = [{K, P} || {P, K} <- Keys0],
    KeySet = sets:from_list([K || {_, K} <- Keys0]),
    {ok, Missing} = foldl(RevTree, Pred, KeySet),
    lists:map(fun(K) -> {proplists:get_value(K, Keys1), K} end, Missing).


%% @doc Get all leaves for the specified Keys.
%% If a key in Keys is an internal node, the this returns
%% all leafs in the subtree for that key.
get_key_leafs(Tree, Keys) ->
    Pred = fun
        ({Depth, Key, _}, branch, {Keys0, Acc0}, {AddKeys0, Path0}) ->
            Keys1 = sets:del_element(Key, Keys0),
            AddKeys1 = (Keys0 == Keys1) orelse AddKeys0, % Key found
            Path1 = [Key | Path0],
            {ok, {Keys1, Acc0}, {AddKeys1, Path1}};
        ({Depth, Key, _}, leaf, {Keys0, Acc0}, {AddKeys0, Path0})
            Keys1 = sets:del_element(Key, Keys0),
            Path1 = [Key | Path0],
            case (Keys0 == Keys1) orelse AddKeys0 of
                true ->
                    Acc1 = [{Val, {Depth, [Key | PAcc]}} | Acc0],
                    {ok, {Keys1, Acc1}, {true, Path1}};
                _ ->
                    {ok, {Keys1, Acc0}, {false, Path1}}
            end
    end,
    KeySet = sets:from_list(Keys),
    foldl(RevTree, Pred, KeySet).
    

%% @doc Get the path for each key in Leaves.
get_paths(RevTree, Leaves) ->
    ok.



%% @doc Get key paths for each key in Keys.
get(RevTree, Keys) ->
    Pred = fun({Pos, [{_Key, Value} | _] = Path}) ->
        {Value, {Pos, [Key || {Key, _} <- Path]}}
    end,
    {ok, {Paths, NotFound}} = get_key_paths(RevTree, Keys),
    {lists:map(Pred, Leaves), NotFound}.


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




