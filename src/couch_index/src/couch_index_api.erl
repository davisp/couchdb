

get(Field, State) ->
    ok.


open(Db, State) ->
    ok.

close(State) ->
    ok.

delete(State) ->
    ok.

reset(State) ->
    ok.


start_update(State) ->
    {ok, State}.

purge(PurgedIdRevs, State) ->
    ok.

process_doc(Doc, State) ->
    ok.

finish_update(State) ->
    {ok, State}.

commit(State) ->
    ok.


compact(Parent, State, Opts) ->
    ok.

swap_compacted(OldState, NewState) ->
    ok.
