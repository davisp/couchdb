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

-module(couch_auth_cache).
-behaviour(gen_server).


-export([start_link/0]).
-export([get_user_creds/1]).
-export([config_change/3]).


-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).


-include("couch_db.hrl").
-include("couch_js_functions.hrl").


-define(BY_USER, auth_cache_by_user).
-define(BY_ATIME, auth_cache_by_atime).


-record(st, {
    db_name,
    update_seq,
    max_size = 0,
    cur_size = 0,
    db_notifier = nil
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


get_user_creds(UserName) when is_list(UserName) ->
    get_user_creds(?l2b(UserName));

get_user_creds(UserName) ->
    UserCreds = case couch_config:get("admins", ?b2l(UserName)) of
        "-hashed-" ++ HashedPwdAndSalt ->
            % This user is an admin, now check to see if there is a user
            % doc which has a matching name, salt, and password_sha
            [HashedPwd, Salt] = string:tokens(HashedPwdAndSalt, ","),
            case get_from_cache(UserName) of
                    nil ->
                        [
                            {<<"roles">>, [<<"_admin">>]},
                            {<<"salt">>, ?l2b(Salt)},
                            {<<"password_sha">>, ?l2b(HashedPwd)}
                        ];
                    Props when is_list(Props) ->
                        DocRoles = couch_util:get_value(<<"roles">>, Props),
                        [
                            {<<"roles">>, [<<"_admin">> | DocRoles]},
                            {<<"salt">>, ?l2b(Salt)},
                            {<<"password_sha">>, ?l2b(HashedPwd)}
                        ]
            end;
        _Else ->
            get_from_cache(UserName)
    end,
    validate_user_creds(UserCreds).


config_change("couch_httpd_auth", "auth_cache_size", SizeList) ->
    Size = list_to_integer(SizeList),
    ok = gen_server:call(?MODULE, {set_cache_size, Size});
config_change("couch_httpd_auth", "authentication_db", DbName) ->
    ok = gen_server:call(?MODULE, {set_db_name, DbName}, infinity).


init(_) ->
    process_flag(trap_exit, true),
    ets:new(?BY_USER, [set, protected, named_table]),
    ets:new(?BY_ATIME, [ordered_set, protected, named_table]),
    ok = couch_config:register(fun ?MODULE:config_change/3),
    DbName = couch_config:get("couch_httpd_auth", "authentication_db"),
    ensure_db_exists(DbName),
    MaxSizeList = couch_config:get("couch_httpd_auth", "auth_cache_size", "50"),
    {ok, Notifier} = couch_db_update_notifier:start_link(fun handle_db_event/1),
    {ok, #st{
        db_name = list_to_binary(DbName),
        update_seq = get_update_seq(DbName),
        max_size = list_to_integer(MaxSizeList),
        cur_size = 0,
        db_notifier = Notifier
    }}.


terminate(_Reason, #st{db_notifier = Notifier}) ->
    couch_db_update_notifier:stop(Notifier),
    ok.


handle_call({set_cache_size, Size}, _From, St) when Size > 0 ->
    {reply, ok, cache_free(St#st{max_size=Size})};

handle_call({set_db_name, DbName}, From, St) when is_list(DbName) ->
    handle_call({set_db_name, list_to_binary(DbName)}, From, St);

handle_call({set_db_name, DbName}, _From, St) when is_binary(DbName) ->
    case DbName == St#st.db_name of
        true -> {reply, ok, St};
        false ->
            ok = ensure_db_exists(DbName),
            {reply, ok, cache_clear(St#st{db_name=DbName})}
    end;

handle_call({lookup, UserName}, _From, St) ->
    % We have to serialize ets operations at this level to
    % guarantee we aren't missing an update. Hence the second
    % ets:lookup/2 even though we know we just failed one.
    {Creds, NewSt} = case ets:lookup(?BY_USER, UserName) of
        [{UserName, {Creds0, ATime}}] ->
            cache_hit(UserName, Creds0, ATime),
            {Creds0, St};
        [] ->
            couch_stats_collector:increment({couchdb, auth_cache_misses}),
            Creds0 = load_from_db(St#st.db_name, UserName),
            St1 = cache_add(UserName, Creds0, erlang:now(), St),
            {Creds0, St1}
    end,
    {reply, Creds, NewSt};

handle_call(Msg, _From, St) ->
    {stop, {invalid_call, Msg}, invalid_call, St}.


handle_cast({cache_hit, UserName, Creds, ATime}, St) ->
    cache_hit(UserName, Creds, ATime),
    {noreply, St};

handle_cast(cache_clear, St) ->
    {noreply, cache_clear(St)};

handle_cast(cache_refresh, St) ->
    {noreply, cache_refresh(St)};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info(Msg, St) ->
    {stop, {invalid_info, Msg}, St}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


get_from_cache(UserName) ->
    case ets:lookup(?BY_USER, UserName) of
        [] ->
            gen_server:call(?MODULE, {lookup, UserName}, infinity);
        [{UserName, {Creds, ATime}}] ->
            couch_stats_collector:increment({couchdb, auth_cache_hits}),
            gen_server:cast(?MODULE, {cache_hit, UserName, Creds, ATime}),
            Creds
    end.


validate_user_creds(nil) ->
    nil;
validate_user_creds(Creds) ->
    case couch_util:get_value(<<"_conflicts">>, Creds) of
        undefined ->
            Creds;
        _ConflictList ->
            throw({unauthorized,
                <<"User document conflicts must be resolved before the "
                 "document is used for authentication purposes.">>
            })
    end.


handle_db_event({Event, UpdatedDbName}) ->
    DbNameList = couch_config:get("couch_httpd_auth", "authentication_db"),
    DbName = list_to_binary(DbNameList),
    case UpdatedDbName == DbName of
        true ->
            case Event of
                created -> gen_server:cast(?MODULE, cache_clear);
                deleted -> gen_server:cast(?MODULE, cache_clear);
                updated -> gen_server:cast(?MODULE, cache_refresh);
                _ -> ok
            end;
        false ->
            ok
    end.


get_update_seq(DbName) when is_list(DbName) ->
    get_update_seq(?l2b(DbName));

get_update_seq(DbName) when is_binary(DbName) ->
    {ok, Db} = open_db(DbName),
    ok = couch_db:close(Db),
    Db#db.update_seq.


load_from_db(DbName, UserName) ->
    {ok, Db} = open_db(DbName),
    DocId = <<"org.couchdb.user:", UserName/binary>>,
    try
        {ok, Doc} = couch_db:open_doc(Db, DocId, [conflicts]),
        user_creds(Doc)
    catch _:_ ->
        nil
    after
        couch_db:close(Db)
    end.


cache_add(_, _, _, #st{max_size=S}=St) when S =< 0 ->
    % Patholigical protection
    St;
cache_add(UserName, Creds, ATime, St0) ->
    St1 = cache_free(St0),
    true = ets:insert(?BY_ATIME, {ATime, UserName}),
    true = ets:insert(?BY_USER, {UserName, {Creds, ATime}}),
    St1#st{cur_size = ets:info(?BY_USER, size)}.


cache_free(#st{max_size=Max, cur_size=Cur}=St) when Cur >= Max ->
    Oldest = ets:last(?BY_ATIME),
    [{Oldest, UserName}] = ets:lookup(?BY_ATIME, Oldest),
    true = ets:delete(?BY_ATIME, Oldest),
    true = ets:delete(?BY_USER, UserName),
    cache_free(St#st{cur_size=Cur-1});
cache_free(St) ->
    St.


cache_clear(St) ->
    true = ets:delete_all_objects(?BY_USER),
    true = ets:delete_all_objects(?BY_ATIME),
    St#st{cur_size = 0}.


cache_hit(UserName, Creds, ATime) ->
    NewATime = erlang:now(),
    true = ets:delete(?BY_ATIME, ATime),
    true = ets:insert(?BY_ATIME, {NewATime, UserName}),
    true = ets:insert(?BY_USER, {UserName, {Creds, NewATime}}).


cache_refresh(St) ->
    {ok, Db} = open_db(St#st.db_name),
    try
        {ok, _, UpdateSeq} = couch_db:enum_docs_since(
            Db,
            St#st.update_seq,
            fun(DocInfo, _, _) -> cache_refresh(Db, DocInfo) end,
            0,
            []
        ),
        St#st{update_seq=UpdateSeq}
    after
        couch_db:close(Db)
    end.


cache_refresh(Db, #doc_info{high_seq = DocSeq} = DocInfo) ->
    case user_name(DocInfo) of
        UserName when is_binary(UserName) ->
            case ets:lookup(?BY_USER, UserName) of
                [{UserName, {_OldCreds, ATime}}] ->
                    Opts = [conflicts, deleted],
                    {ok, Doc} = couch_db:open_doc(Db, DocInfo, Opts),
                    NewCreds = user_creds(Doc),
                    true = ets:insert(?BY_USER, {UserName, {NewCreds, ATime}});
                [] ->
                    ok
            end;
        _ ->
            ok
    end,
    {ok, DocSeq}.


user_name(#doc_info{id = <<"org.couchdb.user:", UserName/binary>>}) ->
    UserName;
user_name(_) ->
    nil.


user_creds(#doc{deleted = true}) ->
    nil;
user_creds(#doc{} = Doc) ->
    {Creds} = couch_doc:to_json_obj(Doc, []),
    Creds.


ensure_db_exists(DbName) when is_list(DbName) ->
    ensure_db_exists(list_to_binary(DbName));

ensure_db_exists(DbName) when is_binary(DbName) ->
    case open_db(DbName) of
        {ok, Db} -> couch_db:close(Db), ok;
        Else -> Else
    end.


open_db(DbName) ->
    Options1 = [sys_db, {user_ctx, #user_ctx{roles=[<<"_admin">>]}}],
    {ok, Db} = case couch_db:open(DbName, Options1) of
        {ok, Db0} -> {ok, Db0};
        _Error -> couch_db:create(DbName, Options1)
    end,
    ok = ensure_auth_ddoc_exists(Db, <<"_design/_auth">>),
    {ok, Db}.


ensure_auth_ddoc_exists(Db, DDocId) ->
    case couch_db:open_doc(Db, DDocId) of
    {not_found, _Reason} ->
        {ok, AuthDesign} = auth_design_doc(DDocId),
        {ok, _Rev} = couch_db:update_doc(Db, AuthDesign, []);
    {ok, Doc} ->
        {Props} = couch_doc:to_json_obj(Doc, []),
        case couch_util:get_value(<<"validate_doc_update">>, Props, []) of
            ?AUTH_DB_DOC_VALIDATE_FUNCTION ->
                ok;
            _ ->
                Props1 = lists:keyreplace(<<"validate_doc_update">>, 1, Props,
                    {<<"validate_doc_update">>,
                    ?AUTH_DB_DOC_VALIDATE_FUNCTION}),
                couch_db:update_doc(Db, couch_doc:from_json_obj({Props1}), [])
        end
    end,
    ok.


auth_design_doc(DocId) ->
    DocProps = [
        {<<"_id">>, DocId},
        {<<"language">>,<<"javascript">>},
        {<<"validate_doc_update">>, ?AUTH_DB_DOC_VALIDATE_FUNCTION}
    ],
    {ok, couch_doc:from_json_obj({DocProps})}.
