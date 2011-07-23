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

-module(couch_index_updater).
-behaviour(gen_server).


%% API
-export([start_link/1, run/2]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).


-record(st, {
    idx,
    mod,
    pid=nil
}).


start_link(Index, Module) ->
    gen_server:start_link(?MODULE, [{Index, Module}], []).


run(Pid, IdxState) ->
    gen_server:call(Pid, {update, IdxState}).


restart(Pid, IdxState) ->
    gen_server:call(Pid, {restart, IdxState}).


init({Index, Module}) ->
    process_flag(trap_exit, true),
    {ok, #st{idx=Index, mod=Module}}.


terminate(_Reason, State) ->
    couch_util:shutdown_sync(State#st.pid),
    ok.


handle_call({update, IdxState}, _From, #st{pid=Pid}=State) when is_pid(Pid) ->
    {reply, ok, State};
handle_call({update, IdxState}, _From, State) ->
    Pid = spawn_link(fun() -> update(State#st.mod, IdxState) end),
    {reply, ok, State#st{pid=Pid}}.


handle_cast(_Mesg, State) ->
    {stop, unknown_cast, State}.


handle_info({'EXIT', Pid, {updated, IdxState}}, #st{pid=Pid}=State) ->
    ok = gen_server:call(State#st.idx, {new_state, IdxState}),
handle_info({'EXIT', Pid, reset}, #st{pid=Pid}=State) ->
    {ok, NewIdxState} = gen_server:call(State#st.idx, reset),
    Pid2 = spawn_link(fun() -> update(State#st.mod, NewIdxState) end),
    {noreply, State#st{pid=Pid2}};
handle_info(_Mesg, State) ->
    {stop, unknown_info, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


update(Mod, IdxState) ->
    Self = self(),
    DbName = Mod:db_name(IdxState),
    CurrSeq = Mod:current_seq(IdxState),
    
    TaskType = <<"Indexer">>,
    Starting = <<"Starting index update.">>,
    couch_task_stats:add_task(TaskType, Mod:index_name(IdxState), Starting),

    couch_util:with_db(DbName, fun(Db) ->
        IdxState1 = case purge_index(Db, Mod, IdxState) of
            {ok, IdxState0} -> IdxState0;
            reset -> exit(reset)
        end,
        
        QueueOpts = [{max_size, 100000}, {max_items, 500}],
        {ok, DocQueue} = couch_work_queue:new(QueueOpts),
        {ok, WriteQueue} = couch_work_queue:new(QueueOpts),
        
        InitIdxState = Mod:prep_update_idx_state(IdxState1),

        DProc = fun() -> process_docs(Mod, IdxState, DocQueue, WriteQueue) end,
        spawn_link(DProc),
        
        WProc = fun() -> write_index(Self, Mod, IdxState, WriteQueue) end,
        spawn_link(WProc),

        UpdateOpts = Mod:update_options(IdxState),
        IncludeDesign = lists:member(UpdateOpts, include_design),
        DocOpts = case lists:member(UpdateOpts, local_seq) of
            true -> [conflicts, deleted_conflicts, local_seq];
            _ -> [conflicts, deleted_conflicts]
        end,
                
        couch_task_status:set_update_frequency(500),
        NumChanges = couch_db:count_changes_since(Db, Seq),

        LoadProc = fun(DocInfo, _, Count) ->
            update_task_status(NumChanges, Count),
            queue_doc(Db, DocInfo, DocOpts, IncludeDesign, DocQueue)
        end,
        {ok, _, _} = couch_db:enum_docs_since(Db, CurrSeq, LoadProc, 0, []),


        couch_work_queue:close(DocQueue),
        couch_task_status:set_udpate_frequency(0),
        couch_task_status:update("Waiting for index writer to finish."),

        receive
            {new_state, NewIdxState} ->
                NewSeq = couch_db:get_update_seq(Db),
                NewIdxState2 = Mod:set_update_seq(NewIdxState, NewSeq),
                exit({updated, NewIdxState2})
        end
    end).



purge_index(Db, Mod, IdxState) ->
    DbPurgeSeq = couch_db:get_purge_seq(Db),
    IdxPurgeSeq = Mod:purge_seq(IdxState),
    if
        DbPurgeSeq == IdxPurgeSeq ->
            {ok, IdxState};
        DbPurgeSeq == IdxPurgeSeq + 1 ->
            couch_task_status:update(<<"Purging index entries.">>),
            {ok, PurgedIdRevs} = couch_db:get_last_purge(Db),
            {ok, NewIdxState} = Mod:purge_index(IdxState, PurgedIdRevs);
        true ->
            couch_task_status:update(<<"Resetting index due to purge state.">>),
            reset
    end.


update_task_status(Total, Count) ->
    PercDone = (Count * 100) div NumChanges,
    Mesg = "Processed ~p of ~p changes (~p%)",
    couch_task_status:update(Mesg, [Count, Total, PercDone]).    


queue_doc(Db, DocInfo, DocOpts, IncludeDesign, DocQueue) ->
    #doc_info{
        id=DocId,
        high_seq=Seq,
        revs=[#rev_info{deleted=Deleted}|_]
    } = DocInfo,
    case {IncludeDesign, DocId} of
        {false, <<"_design/", _/binary>>} ->
            ok
        _ when Deleted ->
            Doc = #doc{id=DocId, deleted=true},
            couch_work_queue:queue(DocQueue, {Seq, Doc});
        _ ->
            {ok, Doc} = couch_db:open_doc_int(Db, DocInfo, DocOpts),
            couch_work_queue:queue(DocQueue, {Seq, Doc})
    end.


process_docs(Mod, IdxState, DocQueue, WriteQueue) ->
    case couch_work_queue:dequeue(DocQueue) of
        closed ->
            couch_work_queue:close(WriteQueue),
            Mod:finish_loading_docs(IdxState, DocAcc),
        {ok, Docs} ->
            {ok, Results, IdxState2} = Mod:process_docs(IdxState, Docs, DocAcc),
            couch_work_queue:queue(WriteQueue, Results),
            process_docs(Mod, IdxState2, DocAcc2, DocQueue, WriteQueue)
    end.


write_index(Parent, Mod, IdxState, WriteQueue) ->
    case couch_work_queue:dequeue(WriteQueue) of
        closed ->
            Parent ! {new_state, IdxState};
        {ok, Items} ->
            {ok, NewIdxState} = Mod:write_entries(IdxState, Items),
            write_index(Parent, Mod, NewIdxState, WriteQueue)
    end.
