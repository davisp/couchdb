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

-module(couch_emsort).

% This is an implementation of an external merge sort. This is used during
% compaction to avoid the cost of maintaining the by_id_btree as an index.
% This module allows us to delay sorting to one single efficient phase at
% the very end of compaction.
%
% This module is more or less a standard N-way external merge sort. The one
% caveat is that it's designed to work with couch_file which adds a step
% in the initial merge phases. Basically, using append only storage we
% can only link to what came before us. This means we need to make sure
% that we have things ordered so when we stream to the client we can stream
% in ascending order.
%
% The basic model for this is to use disk offset pointers as a poor man's
% persistent linked list. The first case that we need to consider is when we
% first write a batch of KVs to disk. This works by sorting the list and
% then we foldr across the list writing each KV with a pointer to the next
% largest KV. The first KV written has a special next value as the atom nil.
% The combination of the KV and the next-pointer will be referred to as a
% kv-triple. When the process of writing kv-triples finishes we end up with
% a linked list stored on disk ordered by key.
%
% Each kv-triple list on disk is then referred to by the smalles valued
% kv-triple. As we write groups of KV's to disk we need to link the heads
% of each list. This is done in much the same way that we manage the
% kv-triples on disk. Each list head is linked to the previous head so that
% we only ever have a single head value in memory at once. A crude ASCII
% art diagram might look like:
%
%     KVP <- KVP <- KVP <- KVP*
%      |      |      |      |
%     KVP    KVP    KVP    KVP
%      |             |      |
%     KVP           KVP    KVP
%      |                    |
%     KVP                  KVP
%
% Each column here is sorted independantly and represents a single call to
% add/2. The order of columns is merely the order in which they were added
% and does not have any sorting information. The KVP marked with an * notes
% the single value we need to store to maintain state (ie commit in a
% header).
%
% Once we have written all of the data that we wish to store we are now
% ready to start in on the sort. There are two basic phases that should
% be familiar to anyone that knows external merges sorts. In a nutshell,
% the first step sorts subsets of data and writes the results back to disk.
% The second and final phase does a final merge of the data in RAM and
% sends the results to a client provided function.
%
% The important thing to remember here is that we're trying to keep the
% amount of RAM used by this sort bounded so that certain inputs won't
% cause us to exceed available memory. In a merge sort the number that
% we need to worry about is the N in the N-way merge. In the ASCII diagram
% above this is basically how many columns we're willing to read from
% at once.
%
% In the first phase of the sort we greedily take N columns and then
% merge them into a new column on disk. For the astute readers, this leads
% us to the one odd caveat about tail-append storage for external merge
% sorts. Namely, as we're writing keys in sorted order, we can't store
% the list in ascending order (because we would have to buffer it in RAM).
% And with tail append storage we can only point to where we know something
% is (ie, backwards).
%
% This leads us to the interesting quandry that after the first phase of
% sorting we'll have columns that end up sorted in reverse order with no
% way to read them in ascending order. If you go and puzzle about this for
% a night while watching Transformers 3 you might come up with the same
% idea I did which is, if sorting reversed the list, why not just sort
% it again to reverse it back?
%
% So that's what we do. Each on-disk sort phase is actually done twice,
% the first pass sorts things with less-than comparisons which gives us
% the correctly ordered (yet incorrected linked) data on disk. The second
% phase is a merge-sort using greater-than preference which maintains our
% correct ordering and leaves us with proper links (yay!).
%
% This process of merging N-columns in two passes happens until the total
% number of columns is less than or equal to N. At this point the disk
% based sorting has finished and we can then stream sorted kv pairs back
% to the user in a single pass with a RAM bounded by N*size(KV).

-export([open/1, open/2, get_fd/1, get_state/1, add/2, sort/3]).


-record(ems, {
    fd,
    root = nil,
    count = 256
}).


open(Fd) ->
    {ok, #ems{fd=Fd}}.


open(Fd, Options) ->
    {ok, set_options(#ems{fd=Fd}, Options)}.


set_options(Ems, []) ->
    Ems;
set_options(Ems, [{root, Root} | Rest]) ->
    set_options(Ems#ems{root=Root}, Rest);
set_options(Ems, [{count, Count} | Rest]) when is_integer(Count) ->
    set_options(Ems#ems{count=Count}, Rest).


get_fd(#ems{fd=Fd}) ->
    Fd.


get_state(#ems{root=Root}) ->
    Root.


add(#ems{root=nil}=Ems, KVs) ->
    Ptr = write_kvs(Ems, KVs),
    {ok, Ems#ems{root={Ptr, nil}}};
add(#ems{root=Prev}=Ems, KVs) ->
    Ptr = write_kvs(Ems, KVs),
    {ok, PPos} = couch_file:append_term(Ems#ems.fd, Prev),
    {ok, Ems#ems{root={Ptr, PPos}}}.


sort(#ems{root=nil}, _Fun, Acc) ->
    Acc;
sort(#ems{}=Ems0, Fun, Acc) ->
    #ems{root={Ptr, Pos}} = Ems1 = decimate(Ems0),
    {Ptrs0, nil} = read_ptrs(Ems1, Pos, [Ptr]),
    Ptrs1 = init_ptrs(small, Ptrs0),
    fold(Ems1, Ptrs1, Fun, Acc).


write_kvs(Ems, KVs) ->
    % Write a list of KV's to disk in sorted order returning
    % a {Key, VPos, NextPos} pointer.
    lists:foldr(fun({K, V}, Acc) ->
        {ok, VPos} = couch_file:append_term(Ems#ems.fd, V),
        case Acc of
            nil ->
                {K, VPos, nil};
            Prev ->
                {ok, PPos} = couch_file:append_term(Ems#ems.fd, Prev),
                {K, VPos, PPos}
        end
    end, nil, lists:sort(KVs)).


decimate(#ems{root={Ptr0, Pos0}}=Ems) ->
    % To make sure we have a bounded amount of data in RAM
    % at any given point we first need to decimate the data
    % by performing the first couple iterations of a merge
    % sort writing the intermediate results back to disk.
    {Ptrs1, LPos1} = read_ptrs(Ems, Pos0, [Ptr0]),
    case LPos1 of
        nil ->
            % We have fewer than count pointers which means we're
            % finished decimating.
            Ems;
        _ ->
            % The first pass gives us a sort with pointers linked from
            % largest to smallest.
            {Ptr2, Pos2} = merge_ptrs(Ems, small, LPos1, Ptrs1),

            % We have to run a second pass so that links are pointed
            % back from smallest to largest.
            {Ptrs2, LPos2} = read_ptrs(Ems, Pos2, [Ptr2]),
            {Ptr3, Pos3} = merge_ptrs(Ems, big, LPos2, Ptrs2),

            % Continue deicmating until we have an acceptable bound
            % on the number of keys to use.
            decimate(Ems#ems{root={Ptr3, Pos3}})
    end.


fold(_Ems, [], _Fun, Acc) ->
    Acc;
fold(Ems, Ptrs0, Fun, Acc0) ->
    % The last step stage of the merge sort. No need
    % to write keys to disk one last time here.
    {Key, VPos, Ptrs1} = choose_ptr(small, Ems, Ptrs0),
    {ok, Val} = couch_file:pread_term(Ems#ems.fd, VPos),
    Acc1 = Fun(Key, Val, Acc0),
    fold(Ems, Ptrs1, Fun, Acc1).


merge_ptrs(Ems, Choose, Pos, Ptrs) ->
    Ptr = merge_lists(Ems, Choose, Ptrs),
    merge_rest_ptrs(Ems, Choose, Pos, {Ptr, nil}).


merge_rest_ptrs(_Ems, _Choose, nil, Acc) ->
    Acc;
merge_rest_ptrs(Ems, Choose, Pos, Prev) ->
    {MPtrs, NPos} = read_ptrs(Ems, Pos, []),
    PtrInfo = merge_lists(Ems, Choose, MPtrs),
    {ok, PPos} = couch_file:append_term(Ems#ems.fd, Prev),
    merge_rest_ptrs(Ems, Choose, NPos, {PtrInfo, PPos}).


read_ptrs(_Ems, nil, Ptrs) ->
    {Ptrs, nil};
read_ptrs(#ems{count=Count}, Pos, Ptrs) when length(Ptrs) >= Count ->
    {Ptrs, Pos};
read_ptrs(Ems, Pos, Ptrs) ->
    {ok, {Ptr, PPos}} = couch_file:pread_term(Ems#ems.fd, Pos),
    read_ptrs(Ems, PPos, [Ptr | Ptrs]).


merge_lists(Ems, Choose, Ptrs0) ->
    Ptrs1 = init_ptrs(Choose, Ptrs0),
    {FKey, FVPos, Ptrs2} = choose_ptr(Choose, Ems, Ptrs1),
    merge_lists(Ems, Choose, Ptrs2, {FKey, FVPos, nil}).


merge_lists(_Ems, _Choose, [], Ptr) ->
    Ptr;
merge_lists(Ems, Choose, Ptrs0, Prev) ->
    {NKey, NVPos, Ptrs1} = choose_ptr(Choose, Ems, Ptrs0),
    {ok, PPos} = couch_file:append_term(Ems#ems.fd, Prev),
    merge_lists(Ems, Choose, Ptrs1, {NKey, NVPos, PPos}).


init_ptrs(small, Ptrs) -> lists:sort(Ptrs);
init_ptrs(big, Ptrs) -> lists:reverse(lists:sort(Ptrs)).


choose_ptr(_Choose, _Ems, [{K, V, nil} | Rest]) ->
    {K, V, Rest};
choose_ptr(Choose, Ems, [{K, V, P} | Rest]) ->
    {ok, Prev} = couch_file:pread_term(Ems#ems.fd, P),
    case Choose of
        small -> {K, V, add_small_ptr(Rest, Prev, [])};
        big -> {K, V, add_big_ptr(Rest, Prev, [])}
    end.


add_small_ptr([P | Rest], Prev, Acc) when P < Prev ->
    add_small_ptr(Rest, Prev, [P | Acc]);
add_small_ptr(Rest, Prev, Acc) ->
    lists:reverse(Acc, [Prev | Rest]).


add_big_ptr([P | Rest], Prev, Acc) when P > Prev ->
    add_big_ptr(Rest, Prev, [P | Acc]);
add_big_ptr(Rest, Prev, Acc) ->
    lists:reverse(Acc, [Prev | Rest]).

