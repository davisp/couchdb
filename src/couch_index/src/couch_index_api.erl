

get(Field, State) ->
    ok.

set(Field, Value, State) ->
    {ok, State}.


open_index(Db, State) ->
    ok.

close_index(State) ->
    ok.

reset_index(State) ->
    ok.


start_update(State) ->
    {ok, State}.

process_doc(Doc, State) ->
    ok.

finish_update(State) ->
    {ok, State}.

purge_index(PurgedIdRevs, State) ->
    ok.

commit(State) ->
    ok.


compact(Parent, State, Opts) ->
    ok.

swap_compacted(OldState, NewState) ->
    ok.
