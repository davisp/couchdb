-module(couch_mrview_compactor).

-export([compact/3, swap_compacted/2]).

compact(Parent, State, Opts) ->
    State.

swap_compacted(OldState, NewState) ->
    NewState.

