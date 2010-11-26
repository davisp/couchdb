-module(erlptr).
-on_load(init/0).


-export([wrap/1, unwrap/1]).


-define(APPNAME, erlptr).
-define(LIBNAME, erlptr).


wrap(Term) ->
    not_loaded(?LINE).

unwrap(Ptr) ->
    not_loaded(?LINE).

%% Internal API

init() ->
    SoName = case code:priv_dir(?APPNAME) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true ->
                    filename:join(["..", priv, ?LIBNAME]);
                _ ->
                    filename:join([priv, ?LIBNAME])
            end;
        Dir ->
            filename:join(Dir, ?LIBNAME)
    end,
    erlang:load_nif(SoName, 0).


not_loaded(Line) ->
    exit({not_loaded, [{module, ?MODULE}, {line, Line}]}).
