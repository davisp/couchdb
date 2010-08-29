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

-module(couch_view_updater).

-export([update/2]).

-include("couch_db.hrl").

-spec update(_, #group{}) -> no_return().

update(Owner, Group) ->
    #group{
        db = #db{name=DbName} = Db,
        name = GroupName,
        current_seq = Seq,
        purge_seq = PurgeSeq
    } = Group,
    couch_task_status:add_task(<<"View Group Indexer">>, <<DbName/binary," ",GroupName/binary>>, <<"Starting index update">>),

    DbPurgeSeq = couch_db:get_purge_seq(Db),
    Group2 =
    if DbPurgeSeq == PurgeSeq ->
        Group;
    DbPurgeSeq == PurgeSeq + 1 ->
        couch_task_status:update(<<"Removing purged entries from view index.">>),
        purge_index(Group);
    true ->
        couch_task_status:update(<<"Resetting view index due to lost purge entries.">>),
        exit(reset)
    end,
    {ok, MapQueue} = couch_work_queue:new(
        [{max_size, 100000}, {max_items, 500}]),
    {ok, WriteQueue} = couch_work_queue:new(
        [{max_size, 100000}, {max_items, 500}]),
    Self = self(),
    ViewEmptyKVs = [{View, []} || View <- Group2#group.views],
    spawn_link(fun() -> do_maps(Group2, MapQueue, WriteQueue, ViewEmptyKVs, false) end),
    spawn_link(fun() -> start_writes(Self, Owner, WriteQueue, Seq == 0) end),
    % compute on all docs modified since we last computed.
    TotalChanges = couch_db:count_changes_since(Db, Seq),
    % update status every half second
    couch_task_status:set_update_frequency(500),
    #group{ design_options = DesignOptions } = Group,
    IncludeDesign = couch_util:get_value(<<"include_design">>,
        DesignOptions, false),
    LocalSeq = couch_util:get_value(<<"local_seq">>, DesignOptions, false),
    DocOpts =
    case LocalSeq of
    true -> [conflicts, deleted_conflicts, local_seq];
    _ -> [conflicts, deleted_conflicts]
    end,
    {ok, _, _}
        = couch_db:enum_docs_since(
            Db,
            Seq,
            fun(DocInfo, _, ChangesProcessed) ->
                couch_task_status:update("Processed ~p of ~p changes (~p%)",
                        [ChangesProcessed, TotalChanges, (ChangesProcessed*100) div TotalChanges]),
                load_doc(Db, DocInfo, MapQueue, DocOpts, IncludeDesign),
                {ok, ChangesProcessed+1}
            end,
            0, []),
    couch_task_status:set_update_frequency(0),
    couch_task_status:update("Finishing."),
    couch_work_queue:close(MapQueue),
    receive {new_group, NewGroup} ->
        exit({new_group,
                NewGroup#group{current_seq=couch_db:get_update_seq(Db)}})
    end.

purge_index(#group{db=Db, views=Views, id_btree=IdBtree}=Group) ->
    {ok, PurgedIdsRevs} = couch_db:get_last_purged(Db),
    Ids = [Id || {Id, _Revs} <- PurgedIdsRevs],
    {ok, Lookups, IdBtree2} = couch_btree:query_modify(IdBtree, Ids, [], Ids),

    % now populate the dictionary with all the keys to delete
    ViewKeysToRemoveDict = lists:foldl(
        fun({ok,{DocId,ViewNumRowKeys}}, ViewDictAcc) ->
            lists:foldl(
                fun({ViewNum, RowKey}, ViewDictAcc2) ->
                    dict:append(ViewNum, {RowKey, DocId}, ViewDictAcc2)
                end, ViewDictAcc, ViewNumRowKeys);
        ({not_found, _}, ViewDictAcc) ->
            ViewDictAcc
        end, dict:new(), Lookups),

    % Now remove the values from the btrees
    Views2 = lists:map(
        fun(#view{id_num=Num,btree=Btree}=View) ->
            case dict:find(Num, ViewKeysToRemoveDict) of
            {ok, RemoveKeys} ->
                {ok, Btree2} = couch_btree:add_remove(Btree, [], RemoveKeys),
                View#view{btree=Btree2};
            error -> % no keys to remove in this view
                View
            end
        end, Views),
    Group#group{
        id_btree=IdBtree2,
        views=Views2,
        purge_seq=couch_db:get_purge_seq(Db)
    }.


load_doc(Db, DocInfo, MapQueue, DocOpts, IncludeDesign) ->
    #doc_info{id=DocId, high_seq=Seq, revs=[#rev_info{deleted=Deleted}|_]} = DocInfo,
    case {IncludeDesign, DocId} of
    {false, <<?DESIGN_DOC_PREFIX, _/binary>>} -> % we skip design docs
        ok;
    _ ->
        if Deleted ->
            couch_work_queue:queue(MapQueue, {Seq, #doc{id=DocId, deleted=true}});
        true ->
            {ok, Doc} = couch_db:open_doc_int(Db, DocInfo, DocOpts),
            couch_work_queue:queue(MapQueue, {Seq, Doc})
        end
    end.
    
do_maps(Group, MapQueue, WriteQueue, ViewEmptyKVs, GroupSent) ->
    case couch_work_queue:dequeue(MapQueue) of
    closed ->
        % We may not need to map any docs, but we still need to
        % pass the group before closing.
        case GroupSent of
            false -> couch_work_queue:queue(WriteQueue, {group, Group});
            _ -> ok
        end,
        couch_work_queue:close(WriteQueue);
    {ok, Queue} ->
        Docs = [Doc || {_,#doc{deleted=false}=Doc} <- Queue],
        DelKVs = [{Id, []} || {_, #doc{deleted=true,id=Id}} <- Queue],
        LastSeq = lists:max([Seq || {Seq, _Doc} <- Queue]),
        {Group1, Results} = view_compute(Group, Docs),
        {ViewKVs, DocIdViewIdKeys} = view_insert_query_results(Docs,
                    Results, ViewEmptyKVs, DelKVs),

        % We have to send the group after we open up the view server.
        case GroupSent of
            false -> couch_work_queue:queue(WriteQueue, {group, Group1});
            _ -> ok
        end,

        couch_work_queue:queue(WriteQueue, {LastSeq, ViewKVs, DocIdViewIdKeys}),
        do_maps(Group1, MapQueue, WriteQueue, ViewEmptyKVs, true)
    end.

% Wait for the mapper process to send us the group with the open
% query server.
start_writes(Parent, Owner, WriteQueue, InitialBuild) ->
    {ok, [{group, Group}]} = couch_work_queue:dequeue(WriteQueue, 1),
    do_writes(Parent, Owner, Group, WriteQueue, InitialBuild).

do_writes(Parent, Owner, Group, WriteQueue, InitialBuild) ->
    case couch_work_queue:dequeue(WriteQueue) of
    closed ->
        Parent ! {new_group, close_view_server(Group)};
    {ok, Queue} ->
        {NewSeq, ViewKeyValues, DocIdViewIdKeys} = lists:foldl(
            fun({Seq, ViewKVs, DocIdViewIdKeys}, nil) ->
                {Seq, ViewKVs, DocIdViewIdKeys};
            ({Seq, ViewKVs, DocIdViewIdKeys}, Acc) ->
                {Seq2, AccViewKVs, AccDocIdViewIdKeys} = Acc,
                AccViewKVs2 = lists:zipwith(
                    fun({View, KVsIn}, {_View, KVsAcc}) ->
                        {View, KVsIn ++ KVsAcc}
                    end, ViewKVs, AccViewKVs),
                {lists:max([Seq, Seq2]),
                        AccViewKVs2, DocIdViewIdKeys ++ AccDocIdViewIdKeys}
            end, nil, Queue),
        Group2 = write_changes(Group, ViewKeyValues, DocIdViewIdKeys, NewSeq,
                InitialBuild),
        case Owner of
            nil ->
                ok;
            _ ->
                % Strip view server references before sending to
                % the group server.
                Group3 = strip_view_server(Group2),
                ok = gen_server:cast(Owner, {partial_update, Parent, Group3})
        end,
        do_writes(Parent, Owner, Group2, WriteQueue, InitialBuild)
    end.

view_insert_query_results([], [], ViewKVs, DocIdViewIdKeysAcc) ->
    {ViewKVs, DocIdViewIdKeysAcc};
view_insert_query_results([Doc|RestDocs], [QueryResults | RestResults], ViewKVs, DocIdViewIdKeysAcc) ->
    {NewViewKVs, NewViewIdKeys} = view_insert_doc_query_results(Doc, QueryResults, ViewKVs, [], []),
    NewDocIdViewIdKeys = [{Doc#doc.id, NewViewIdKeys} | DocIdViewIdKeysAcc],
    view_insert_query_results(RestDocs, RestResults, NewViewKVs, NewDocIdViewIdKeys).


view_insert_doc_query_results(_Doc, [], [], ViewKVsAcc, ViewIdKeysAcc) ->
    {lists:reverse(ViewKVsAcc), lists:reverse(ViewIdKeysAcc)};
view_insert_doc_query_results(#doc{id=DocId}=Doc, [ResultKVs|RestResults], [{View, KVs}|RestViewKVs], ViewKVsAcc, ViewIdKeysAcc) ->
    % Take any identical keys and combine the values
    ResultKVs2 = lists:foldl(
        fun({Key,Value}, [{PrevKey,PrevVal}|AccRest]) ->
            case Key == PrevKey of
            true ->
                case PrevVal of
                {dups, Dups} ->
                    [{PrevKey, {dups, [Value|Dups]}} | AccRest];
                _ ->
                    [{PrevKey, {dups, [Value,PrevVal]}} | AccRest]
                end;
            false ->
                [{Key,Value},{PrevKey,PrevVal}|AccRest]
            end;
        (KV, []) ->
           [KV]
        end, [], lists:sort(ResultKVs)),
    NewKVs = [{{Key, DocId}, Value} || {Key, Value} <- ResultKVs2],
    NewViewKVsAcc = [{View, NewKVs ++ KVs} | ViewKVsAcc],
    NewViewIdKeys = [{View#view.id_num, Key} || {Key, _Value} <- ResultKVs2],
    NewViewIdKeysAcc = NewViewIdKeys ++ ViewIdKeysAcc,
    view_insert_doc_query_results(Doc, RestResults, RestViewKVs, NewViewKVsAcc, NewViewIdKeysAcc).

view_compute(Group, []) ->
    {Group, []};
view_compute(Group, Docs) ->
    {Group2, Server} = get_view_server(Group),
    {ok, Results} = couch_view_server:map(Server, Docs),
    {Group2, Results}.


write_changes(Group, ViewKeyValuesToAdd, DocIdViewIdKeys, NewSeq, InitialBuild) ->
    #group{id_btree=IdBtree} = Group,

    AddDocIdViewIdKeys = [{DocId, ViewIdKeys} || {DocId, ViewIdKeys} <- DocIdViewIdKeys, ViewIdKeys /= []],
    if InitialBuild ->
        RemoveDocIds = [],
        LookupDocIds = [];
    true ->
        RemoveDocIds = [DocId || {DocId, ViewIdKeys} <- DocIdViewIdKeys, ViewIdKeys == []],
        LookupDocIds = [DocId || {DocId, _ViewIdKeys} <- DocIdViewIdKeys]
    end,
    {ok, LookupResults, IdBtree2}
        = couch_btree:query_modify(IdBtree, LookupDocIds, AddDocIdViewIdKeys, RemoveDocIds),
    KeysToRemoveByView = lists:foldl(
        fun(LookupResult, KeysToRemoveByViewAcc) ->
            case LookupResult of
            {ok, {DocId, ViewIdKeys}} ->
                lists:foldl(
                    fun({ViewId, Key}, KeysToRemoveByViewAcc2) ->
                        dict:append(ViewId, {Key, DocId}, KeysToRemoveByViewAcc2)
                    end,
                    KeysToRemoveByViewAcc, ViewIdKeys);
            {not_found, _} ->
                KeysToRemoveByViewAcc
            end
        end,
        dict:new(), LookupResults),
    Views2 = lists:zipwith(fun(View, {_View, AddKeyValues}) ->
            KeysToRemove = couch_util:dict_find(View#view.id_num, KeysToRemoveByView, []),
            {ok, ViewBtree2} = couch_btree:add_remove(View#view.btree, AddKeyValues, KeysToRemove),
            View#view{btree = ViewBtree2}
        end,    Group#group.views, ViewKeyValuesToAdd),
    Group#group{views=Views2, current_seq=NewSeq, id_btree=IdBtree2}.


% Prepare a view server for map/reduce work. Compile the necessary
% functions and update the btree's that we're going to be writing
% to. We store a copy of the reduce function that was generated in
% couch_view_group:init_group/4 so that we can replace this version
% when we send the group back. This way view readers aren't using
% the same view server process that we are.
get_view_server(#group{view_server=Server}=Group) when Server =/= nil ->
    {Group, Server};
get_view_server(Group) ->
    #group{
        def_lang=Lang,
        views=Views,
        view_server=nil
    } = Group,

    % Gather functions to compile.
    {MapFuns, RedFuns} = lists:foldl(fun(View, {MapAcc, RedAcc}) ->
        ViewId = View#view.id_num,
        MapFun = View#view.def,
        RedFuns = [FunSrc || {_Name, FunSrc} <- View#view.reduce_funs],
        {[MapFun | MapAcc], [[ViewId, RedFuns] | RedAcc]}
    end, {[], []}, Views),
    
    RevMapFuns = lists:reverse(MapFuns), % Order for maps does matter.
    
    {ok, Server} = couch_view_server:get_server(Lang, RevMapFuns, RedFuns),
    
    % Rebuild the reduce functions
    Views2 = lists:map(fun(View) ->
        #view{
            id_num=ViewId,
            reduce_funs=RedFuns2,
            btree=Btree
        } = View,
        
        FunSrcs = [FunSrc || {_Name, FunSrc} <- RedFuns2],
        ReduceFun = fun
            (reduce, KVs) ->
                KVs2 = couch_view:expand_dups(KVs,[]),
                KVs3 = couch_view:detuple_kvs(KVs2,[]),
                {ok, Reduced} = couch_view_server:reduce(Server, ViewId, FunSrcs, KVs3),
                {length(KVs3), Reduced};
            (rereduce, Reds) ->
                Count = lists:sum([Count0 || {Count0, _} <- Reds]),
                UserReds = [UserRedsList || {_, UserRedsList} <- Reds],
                {ok, Reduced} = couch_view_server:rereduce(Server, ViewId, FunSrcs, UserReds),
                {Count, Reduced}
        end,

        OldRed = couch_btree:get_reduce(Btree),
        Btree2 = couch_btree:set_options(Btree, [{reduce, ReduceFun}]),

        View#view{btree=Btree2, native_red_fun=OldRed}
    end, Views),

    {Group#group{views=Views2, view_server=Server}, Server}.
    
% Before sending a group back to the group server, we strip
% any references to the view server so we don't leak any
% handles to it.
strip_view_server(#group{views=Views}=Group) ->
    Views2 = lists:map(fun(View) ->
        #view{
            btree=Btree,
            native_red_fun=RedFun
        } = View,
        Btree2 = couch_btree:set_options(Btree, [{reduce, RedFun}]),
        View#view{btree=Btree2}
    end, Views),
    Group#group{view_server=nil, views=Views2}.

% We finished updating the view at the end of do_writes. Now
% we close the view and strip references before sending back.
close_view_server(#group{view_server=nil}=Group) ->
    % We never started a view server, nothing to release.
    Group;
close_view_server(#group{view_server=Server}=Group) ->
    couch_view_server:ret_server(Server),
    strip_view_server(Group#group{view_server=nil}).
