-module(couch_mrview_updater).

-export([purge_index/3, process_doc/2, finish_update/2]).

purge_index(_Db, PurgeSeq, PurgedIdRevs, State) ->
    #mrst{
        id_btree=IdBtree,
        views=Views
    } = State,

    MakeDictFun = fun
        ({ok, {DocId, ViewNumRowKeys}}, DictAcc) ->
            FoldFun = fun({ViewNum, RowKey}, DictAcc2) ->
                dict:append(ViewNum, {RowKey, DocId}, DictAcc2)
            end,
            lists:foldl(FoldFun, DictAcc, ViewNumRowKeys);
        ({not_found, _}, DictAcc) ->
            DictAcc
    end,

    RemKeysFun = fun(#view{id_num=Num, btree=Btree}=View) ->
        case dict:find(Num, KeysToRemove) of
            {ok, RemKeys} ->
                {ok, ViewBtree2} = couch_btree:add_remove(Btree, [], RemKeys),
                View#view{btree=ViewBtree2};
            error ->
                View
        end
    end,

    Ids = [Id || {Id, _Revs} <- PurgedIdRevs],
    {ok, Lookups, IdBtree2} = couch_btree:query_modify(IdBtree, Ids, [], Ids),
    KeysToRemove = lists:foldl(MakeDictFun, dict:new(), Lookups),
    Views2 = lists:map(RemKeysFun, Views)
    State#mrst{
        id_btree=IdBtree2,
        views=Views2,
        purge_seq=PurgeSeq
    }.


process_doc(Doc, #mrst{query_server=nil}=State) ->
    process_doc(Doc, start_query_server(State));
process_doc(#doc{id=DocId, deleted=false}, #mrst{views=Views}=State) ->
    {ok, [Results]} = couch_query_servers:map_docs(QueryServer, [Doc]),
    {ViewKVs, DocIdKeys} = process_results(DocId, Views, Results, {[], []}),
    couch_work_queue:queue(State#mrst.write_queue, {ViewKVs, DocIdKeys}),
    State;
process_doc(#doc{id=Id, deleted=true}, State) ->
    couch_work_queue:queue(State#mrst.write_queue, {[], [{Id, []}]}),
    State.


finish_update(Parent, State) ->
    ok.


start_query_server(State) ->
    #mrst{
        language=Language,
        lib=Lib,
        views=Views
    } = State,
    Defs = [View#view.def || View <- Views],
    {ok, QServer} = couch_query_servers:start_doc_map(Language, Defs, Lib),

    QueueOpts = [{max_size, 100000}, {max_items, 500}],
    {ok, Queue} = couch_work_queue:new(QueueOpts),

    Self = self(),
    WriteFun = fun() -> write_results(Self, Mod, IdxState, Queue) end,
    spawn_link(WriteFun),

    State#mrst{
        query_server=QServer,
        write_queue=Queue,
    }.


process_results(DocId, [V | RViews], [KVs | RKVs], {KVAcc, ByIdAcc}) ->
    CombineDupesFun = fun
        ({Key, Val}, [{Key, {dups, Vals}} | Rest]) ->
            [{Key, {dups, [Val | Dups]}} | Rest];
        ({Key, Val1}, [{Key, Val2} | Rest]) ->
            [{Key, {dups, [Val1, Val2]}} | Rest];
        (KV, Rest) ->
            [KV | Rest]
    end,
    Duped = lists:fold(CombineDupesFun, [], lists:sort(KVs)),

    {ViewKVAcc, DocIdKeyAcc} = Acc,
    ViewKVs0 = {V#view.id_num, [{{Key, DocId}, Value} || {Key, Val} <- Duped]},
    ViewKVs = [ViewKVs0 | KVAcc],
    DocIdKeys = [{V#view.id_num, Key} || {Key, _} <- Duped] ++ DocIdKeyAcc,
    process_results(DocId, RViews, RKVs, {ViewKVs, DocIdKeys}).


write_results(Parent, Owner, Group) ->
    case couch_work_queue:dequeue(WriteQueue) of
    closed ->
        Parent ! {new_group, Group};
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
        nil -> ok;
        _ -> ok = gen_server:cast(Owner, {partial_update, Parent, Group2})
        end,
        do_writes(Parent, Owner, Group2, WriteQueue, InitialBuild)
    end.




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
            case ViewBtree2 =/= View#view.btree of
                true ->
                    View#view{btree=ViewBtree2, update_seq=NewSeq};
                _ ->
                    View#view{btree=ViewBtree2}
            end
        end,    Group#group.views, ViewKeyValuesToAdd),
    Group#group{views=Views2, current_seq=NewSeq, id_btree=IdBtree2}.

