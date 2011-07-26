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


start_update(Parent, Partial, State) ->
    State#mrst{
        updater_pid=Parent,
        partial_resp_pid=Partial
    }.


finish_update(State) ->
    receive
        {new_state, State} ->
            State
    end.


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
        initial_build=State#mrst.update_seq==0
    }.


process_results(_, [], [], {KVAcc, ByIdAcc}) ->
    {lists:reverse(KVAcc), ByIdAcc};
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


write_results(Parent, Mod, State, Queue) ->
    case couch_work_queue:dequeue(Queue) of
        closed ->
            Parent ! {new_state, IdxState};
        {ok, Results} ->
            {ViewKVs, DocIdKeys} = lists:foldl(fun merge_kvs/2, nil, Results),
            NewState = write_kvs(State, ViewKVs, DocIdKeys),
            send_partial(NewIdxState#mrst.partial_resp_pid, NewState),
            write_results(Parent, Mod, NewState, Queue)
    end.


write_kvs(State, ViewKVs, DocIdKeys) ->
    #mrst{
        id_btree=IdBtree,
        initial_build=InitialBuild
    } = State,
    
    UpdateByDocIdBtree = fun
        (Btree, DocIdKeys, true) ->
            ToAdd = [{Id, DIKeys} || {Id, DIKeys} <- DocIdKeys, DIKeys /= []],
            couch_btree:query_modify(Btree, [], ToAdd, []);
        (Btree, DocIdKeys, _) ->
            ToFind = [Id || {DocId, _} <- DocIdKeys]
            ToAdd = [{Id, DIKeys} || {Id, DIKeys} <- DocIdKeys, DIKeys /= []],
            ToRem = [Id || {DocId, DIKeys} <- DocIdKeys, DIKeys == []],
            couch_btree:query_modify(IdBtree, ToFind, ToAdd, ToRem)
    end,
    
    CollectRemKeys = fun(KeysToRem, RemAcc) ->
        case KeysToRem of
            {ok, {DocId, ViewIdKeys}} ->
                FoldFun = fun({ViewId, Key}, RemAcc2) ->
                    dict:append(ViewId, {Key, DocId}, RemAcc2)
                end,
                lists:foldl(FoldFun, RemAcc, ViewIdKeys);
            {not_found, _} ->
                RemAcc
        end
    end,
    
    UpdateViewFun = fun(View, ViewKVsPerView) ->
        KeysToRem = couch_util:dict_find(View#view.id_num, )
    
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
    
    
    
    
    {ok, Results, IdBtree} = update_by_docid(IdBtree, DocIdKeys, InitialBuild),
    
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


merge_kvs([{ViewKVs, DocIdKeys} | Rest], nil) ->
    combine_results(Rest, {ViewKVs, DocIdKeys});
merge_kvs([{ViewKVs, DocIdKeys} | Rest], {ViewKVsAcc, DocIdKeysAcc}) ->
    KVCombine = fun({ViewNum, KVs}, {ViewNum, KVsAcc}) ->
        {ViewNum, KVs ++ KVsAcc}
    end,
    ViewKVs2 = lists:zipwith(KVCombin, ViewKVs, ViewKVsAcc),
    {ViewKVs2, DocIdKeys ++ DocIdKeysAcc}.


send_partial(Pid, State) when is_pid(Pid) ->
    Pid ! {parital_update, State};
send_partial(_, _) ->
    ok.
    