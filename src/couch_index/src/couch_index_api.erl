
db_name(State) ->
    ok.

index_name(State) ->
    ok.

signature(State) ->
    ok.

update_seq(State) ->
    ok.

set_update_seq(Seq, State) ->
    ok.

purge_seq(State) ->
    ok.



open_index(Db, State) ->
    ok.

close_index(State) ->
    ok.


update_options(State) ->
    ok.

process_doc(Doc, State) ->
    ok.

purge_index(PurgedIdRevs, State) ->
    ok.

commit(State) ->
    ok.


compact(Parent, State, Opts) ->
    ok.


swap_compacted(OldState, NewState) ->
    ok.


reset_index(State) ->
    ok.