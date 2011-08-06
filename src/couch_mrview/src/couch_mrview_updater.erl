-module(couch_mrview_updater).

-export([start_update/3, process_doc/3, finish_update/1, purge_index/4]).

-include("couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").


start_update(Parent, Partial, State) ->
    QueueOpts = [{max_size, 100000}, {max_items, 500}],
    {ok, Queue} = couch_work_queue:new(QueueOpts),

    Self = self(),
    WriteFun = fun() -> write_results(Self, State, Queue) end,
    spawn_link(WriteFun),

    State#mrst{
        updater_pid=Parent,
        partial_resp_pid=Partial,
        write_queue=Queue
    }.


process_doc(Doc, Seq, #mrst{query_server=nil}=State) ->
    process_doc(Doc, Seq, start_query_server(State));
process_doc(#doc{id=DocId, deleted=false}=Doc, Seq, State) ->
    #mrst{views=Views, query_server=QServer} = State,
    {ok, [Results]} = couch_query_servers:map_docs(QServer, [Doc]),
    {ViewKVs, DocIdKeys} = process_results(DocId, Views, Results, {[], []}),
    couch_work_queue:queue(State#mrst.write_queue, {Seq, ViewKVs, DocIdKeys}),
    State;
process_doc(#doc{id=Id, deleted=true}, Seq, State) ->
    KVs = [{V#mrview.id_num, []} || V <- State#mrst.views],
    couch_work_queue:queue(State#mrst.write_queue, {Seq, KVs, [{Id, []}]}),
    State.


finish_update(State) ->
    couch_work_queue:close(State#mrst.write_queue),
    receive
        {new_state, State} ->
            State
    end.


purge_index(_Db, PurgeSeq, PurgedIdRevs, State) ->
    #mrst{
        id_btree=IdBtree,
        views=Views
    } = State,

    Ids = [Id || {Id, _Revs} <- PurgedIdRevs],
    {ok, Lookups, IdBtree2} = couch_btree:query_modify(IdBtree, Ids, [], Ids),

    MakeDictFun = fun
        ({ok, {DocId, ViewNumRowKeys}}, DictAcc) ->
            FoldFun = fun({ViewNum, RowKey}, DictAcc2) ->
                dict:append(ViewNum, {RowKey, DocId}, DictAcc2)
            end,
            lists:foldl(FoldFun, DictAcc, ViewNumRowKeys);
        ({not_found, _}, DictAcc) ->
            DictAcc
    end,
    KeysToRemove = lists:foldl(MakeDictFun, dict:new(), Lookups),

    RemKeysFun = fun(#mrview{id_num=Num, btree=Btree}=View) ->
        case dict:find(Num, KeysToRemove) of
            {ok, RemKeys} ->
                {ok, Btree2} = couch_btree:add_remove(Btree, [], RemKeys),
                NewPurgeSeq = case Btree2 /= Btree of
                    true -> PurgeSeq;
                    _ -> View#mrview.purge_seq
                end,
                View#mrview{btree=Btree2, purge_seq=NewPurgeSeq};
            error ->
                View
        end
    end,

    Views2 = lists:map(RemKeysFun, Views),
    {ok, State#mrst{
        id_btree=IdBtree2,
        views=Views2,
        purge_seq=PurgeSeq
    }}.


start_query_server(State) ->
    #mrst{
        language=Language,
        lib=Lib,
        views=Views
    } = State,
    Defs = [View#mrview.def || View <- Views],
    {ok, QServer} = couch_query_servers:start_doc_map(Language, Defs, Lib),

    State#mrst{
        query_server=QServer,
        first_build=State#mrst.update_seq==0
    }.


process_results(_, [], [], {KVAcc, ByIdAcc}) ->
    {lists:reverse(KVAcc), ByIdAcc};
process_results(DocId, [V | RViews], [KVs | RKVs], {KVAcc, ByIdAcc}) ->
    CombineDupesFun = fun
        ({Key, Val}, [{Key, {dups, Vals}} | Rest]) ->
            [{Key, {dups, [Val | Vals]}} | Rest];
        ({Key, Val1}, [{Key, Val2} | Rest]) ->
            [{Key, {dups, [Val1, Val2]}} | Rest];
        (KV, Rest) ->
            [KV | Rest]
    end,
    Duped = lists:foldl(CombineDupesFun, [], lists:sort(KVs)),

    ViewKVs0 = {V#mrview.id_num, [{{Key, DocId}, Val} || {Key, Val} <- Duped]},
    ViewKVs = [ViewKVs0 | KVAcc],
    DocIdKeys = [{DocId, [{V#mrview.id_num, Key} || {Key, _} <- Duped]}],
    process_results(DocId, RViews, RKVs, {ViewKVs, DocIdKeys ++ ByIdAcc}).


write_results(Parent, State, Queue) ->
    case couch_work_queue:dequeue(Queue) of
        closed ->
            Parent ! {new_state, State};
        {ok, Info} ->
            {Seq, ViewKVs, DocIdKeys} = lists:foldl(fun merge_kvs/2, nil, Info),
            NewState = write_kvs(State, Seq, ViewKVs, DocIdKeys),
            send_partial(NewState#mrst.partial_resp_pid, NewState),
            write_results(Parent, NewState, Queue)
    end.


merge_kvs({Seq, ViewKVs, DocIdKeys}, nil) ->
    {Seq, ViewKVs, DocIdKeys};
merge_kvs({Seq, ViewKVs, DocIdKeys}, {SeqAcc, ViewKVsAcc, DocIdKeysAcc}) ->
    KVCombine = fun({ViewNum, KVs}, {ViewNum, KVsAcc}) ->
        {ViewNum, KVs ++ KVsAcc}
    end,
    ViewKVs2 = lists:zipwith(KVCombine, ViewKVs, ViewKVsAcc),
    {lists:max([Seq, SeqAcc]), ViewKVs2, DocIdKeys ++ DocIdKeysAcc}.


write_kvs(State, UpdateSeq, ViewKVs, DocIdKeys) ->
    #mrst{
        id_btree=IdBtree,
        first_build=FirstBuild
    } = State,
    
    {ok, ToRemove, IdBtree2} = update_id_btree(IdBtree, DocIdKeys, FirstBuild),
    ToRemByView = collapse_rem_keys(ToRemove, dict:new()),

    UpdateView = fun(#mrview{id_num=ViewId}=View, {ViewId, KVs}) ->
        ToRem = couch_util:dict_find(ViewId, ToRemByView, []),
        {ok, VBtree2} = couch_btree:add_remove(View#mrview.btree, KVs, ToRem),
        case VBtree2 =/= View#mrview.btree of
            true -> View#mrview{btree=VBtree2, update_seq=UpdateSeq};
            _ -> View
        end
    end,

    State#mrst{
        views=lists:zipwith(UpdateView, State#mrst.views, ViewKVs),
        update_seq=UpdateSeq,
        id_btree=IdBtree2
    }.


update_id_btree(Btree, DocIdKeys, true) ->
    ToAdd = [{Id, DIKeys} || {Id, DIKeys} <- DocIdKeys, DIKeys /= []],
    couch_btree:query_modify(Btree, [], ToAdd, []);
update_id_btree(Btree, DocIdKeys, _) ->
    ToFind = [Id || {Id, _} <- DocIdKeys],
    ToAdd = [{Id, DIKeys} || {Id, DIKeys} <- DocIdKeys, DIKeys /= []],
    ToRem = [Id || {Id, DIKeys} <- DocIdKeys, DIKeys == []],
    couch_btree:query_modify(Btree, ToFind, ToAdd, ToRem).


collapse_rem_keys([], Acc) ->
    Acc;
collapse_rem_keys([{ok, {DocId, ViewIdKeys}} | Rest], Acc) ->
    NewAcc = lists:foldl(fun({ViewId, Key}, Acc2) -> 
        dict:append(ViewId, {Key, DocId}, Acc2)
    end, Acc, ViewIdKeys),
    collapse_rem_keys(Rest, NewAcc);
collapse_rem_keys([{not_found, _} | Rest], Acc) ->
    collapse_rem_keys(Rest, Acc).


send_partial(Pid, State) when is_pid(Pid) ->
    Pid ! {parital_update, State};
send_partial(_, _) ->
    ok.
    
