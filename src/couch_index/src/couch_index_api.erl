
db_name(State) ->
    ok.

index_name(State) ->
    ok.

signature(State) ->
    ok.

update_seq(State) ->
    ok.

set_update_seq(State, Seq) ->
    ok.

purge_seq(State) ->
    ok.



open_index(State) ->
    ok.

close_index(State) ->
    ok.


commit(State) ->
    ok.


update(Parent, State) ->
    ok.

update_options(State) ->
    ok.

process_docs(State, Docs) ->
    ok.

finish_loading_docs(State) ->
    ok.

write_entries(State, Entries) ->
    ok.

purge_index(State, PurgedIdRevs) ->
    ok.


compact(Parent, State) ->
    ok.

recompact(Parent, State) ->
    ok.

switch_compacted(OldState, NewState) ->
    ok.


reset_index(State) ->
    ok.