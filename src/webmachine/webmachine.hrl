-export([ping/2]).

-include_lib("webmachine/wm_reqdata.hrl").

ping(ReqData, State) ->
    {pong, ReqData, State}.


