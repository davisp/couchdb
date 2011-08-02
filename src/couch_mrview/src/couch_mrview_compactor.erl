-module(couch_mrview_compactor).

-export([compact/3, swap_compacted/2]).

compact(_Parent, State, _Opts) ->
    State.

swap_compacted(_OldState, NewState) ->
    NewState.

