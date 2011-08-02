-module(couch_mrview_util).

-export([get_view/4, reset_index/3]).
-export([make_header/1]).
-export([open_index_file/3, open_compaction_file/3]).
-export([get_row_count/1, reduce_to_count/1]).
-export([key_opts/1]).
-export([fold/4]).
-export([maybe_load_doc/4]).


-include("couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").


get_view(DbName, DDoc, ViewName, Args) when is_binary(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        get_view(Db, DDoc, ViewName, Args)
    end);
get_view(Db, DDoc, ViewName, Args) when is_binary(DDoc) ->
    case couch_db:open_doc(Db, DDoc, [ejson_body]) of
        {ok, Doc} -> get_view(Db, Doc, ViewName, Args);
        Error -> Error
    end;
get_view(Db, DDoc, ViewName, Args0) ->
    InitState = ddoc_to_mrst(couch_db:name(Db), DDoc),
    Args = set_view_type(Args0, ViewName, InitState#mrst.views),
    validate_args(Args),
    MinSeq = case Args#mrargs.stale of
        ok -> 0;
        update_after -> 0;
        _ -> couch_db:get_update_seq(Db)
    end,
    {ok, Pid} = couch_index_server:get_index(couch_mrview_index, InitState),
    {ok, State} = couch_index:get_state(Pid, MinSeq),
    erlang:monitor(process, State#mrst.fd),
    extract_view(Args, ViewName, State#mrst.views).


ddoc_to_mrst(DbName, #doc{id=Id, body={Fields}}) ->
    MakeDict = fun({Name, {MRFuns}}, DictBySrcAcc) ->
        case couch_util:get_value(<<"map">>, MRFuns) of
            MapSrc when is_binary(MapSrc) ->
                RedSrc = couch_util:get_value(<<"reduce">>, MRFuns, null),
                {ViewOpts} = couch_util:get_value(<<"options">>, MRFuns, {[]}),
                View = case dict:find({MapSrc, ViewOpts}, DictBySrcAcc) of
                    {ok, View0} -> View0;
                    error -> #mrview{def=MapSrc, options=ViewOpts}
                end,
                {MapNames, RedSrcs} = case RedSrc of
                    null ->
                        MNames = [Name | View#mrview.map_names],
                        {MNames, View#mrview.reduce_funs};
                    _ ->
                        RedFuns = [{Name, RedSrc} | View#mrview.reduce_funs],
                        {View#mrview.map_names, RedFuns}
                end,
                View2 = View#mrview{map_names=MapNames, reduce_funs=RedSrcs},
                dict:store({MapSrc, ViewOpts}, View2, DictBySrcAcc);
            undefined ->
                DictBySrcAcc
        end
    end,
    {RawViews} = couch_util:get_value(<<"views">>, Fields, {[]}),
    BySrc = lists:foldl(MakeDict, dict:new(), RawViews),

    NumViews = fun({_, View}, N) -> {View#mrview{id_num=N}, N+1} end,
    {Views, _} = lists:mapfoldl(NumViews, 0, lists:sort(dict:to_list(BySrc))),

    Language = couch_util:get_value(<<"language">>, Fields, <<"javascript">>),
    {DesignOpts} = couch_util:get_value(<<"options">>, Fields, {[]}),
    {RawViews} = couch_util:get_value(<<"views">>, Fields, {[]}),    
    Lib = couch_util:get_value(<<"lib">>, RawViews, {[]}),

    RootDir = couch_config:get("couchdb", "index_dir"),

    IdxState = #mrst{
        db_name=DbName,
        idx_name=Id,
        lib=Lib,
        views=Views, 
        language=Language,
        design_opts=DesignOpts,
        root_dir=RootDir
    },
    set_view_sig(IdxState).


set_view_sig(State) ->
    #mrst{
        views=Views,
        lib=Lib,
        language=Language,
        design_opts=DesignOptions
    } = State,
    SigInfo = {Views, Language, DesignOptions, sort_lib(Lib)},
    State#mrst{sig=couch_util:md5(term_to_binary(SigInfo))}.


set_view_type(_Args, _ViewName, []) ->
    throw({not_found, missing_named_view});
set_view_type(Args, ViewName, [View | Rest]) ->
    RedNames = [N || {N, _} <- View#mrview.reduce_funs],
    case lists:member(ViewName, RedNames) of
        true ->
            case Args#mrargs.reduce of
                true -> Args#mrargs{view_type=reduce};
                false -> Args#mrargs{view_type=map}
            end;
        false ->
            case lists:member(ViewName, View#mrview.map_names) of
                true -> Args#mrargs{view_type=map};
                false -> set_view_type(Args, ViewName, Rest)
            end
    end.


extract_view(_Args, _ViewName, []) ->
    throw({not_found, missing_named_view});
extract_view(#mrargs{view_type=map}=Args, Name, [View | Rest]) ->
    Names = View#mrview.map_names ++ [N || {N, _} <- View#mrview.reduce_funs],
    case lists:member(Name, Names) of
        true -> {map, View, Args};
        _ -> extract_view(Args, Name, Rest)
    end;
extract_view(#mrargs{view_type=red}=Args, Name, [View | Rest]) ->
    RedNames = [N || {N, _} <- View#mrview.reduce_funs],
    case lists:member(Name, RedNames) of
        true -> {red, {index_of(Name, RedNames), View}, Args};
        false -> extract_view(Args, Name, Rest)
    end.


validate_args(Args) ->
    case is_boolean(Args#mrargs.reduce) of
        true -> ok;
        _ -> mrverror(<<"Invalid `reduce` value.">>)
    end,

    case Args#mrargs.keys of
        Keys when is_list(Keys) -> ok;
        _ -> mrverror(<<"`keys` must be an array of strings.">>)
    end,

    case {Args#mrargs.keys, Args#mrargs.start_key} of
        {[], _} -> ok;
        {[_|_], undefined} -> ok;
        _ -> mrverror(<<"`start_key` is incompatible with `keys`">>)
    end,

    case {Args#mrargs.keys, Args#mrargs.end_key} of
        {[], _} -> ok;
        {[_|_], undefined} -> ok;
        _ -> mrverror(<<"`end_key` is incompatible with `keys`">>)
    end,

    case Args#mrargs.direction of
        fwd -> ok;
        rev -> ok;
        _ -> mrverror(<<"Invalid direction.">>)
    end,

    case {Args#mrargs.limit >= 0, Args#mrargs.limit == undefined} of
        {true, _} -> ok;
        {_, true} -> ok;
        _ -> mrverror(<<"`limit` must be a positive integer.">>)
    end,

    case Args#mrargs.skip < 0 of
        true -> mrverror(<<"`skip` must be >= 0">>);
        _ -> ok
    end,

    case {Args#mrargs.view_type, Args#mrargs.group_level} of
        {_, 0} -> ok;
        {reduce, Int} when is_integer(Int), Int >= 0 -> ok;
        {reduce, _} -> mrverror(<<"`group_level` must be >= 0">>);
        {map, _} -> mrverror(<<"Invalid use of grouping on a map view.">>)
    end,

    case Args#mrargs.stale of
        ok -> ok;
        update_after -> ok;
        false -> ok;
        _ -> mrverror(<<"Invalid value for `stale`.">>)
    end,

    case is_boolean(Args#mrargs.inclusive_end) of
        true -> ok;
        _ -> mrverror(<<"Invalid value for `inclusive_end`.">>)
    end,

    case is_boolean(Args#mrargs.include_docs) of
        true -> ok;
        _ -> mrverror(<<"Invalid value for `include_docs`.">>)
    end,

    case {Args#mrargs.view_type, Args#mrargs.conflicts} of
        {_, undefined} -> ok;
        {map, V} when is_boolean(V) -> ok;
        {reduce, undefined} -> ok;
        {map, _} -> mrverror(<<"Invalid value for `conflicts`.">>);
        {reduce, _} -> mrverror(<<"`conflicts` is invalid for reduce views.">>)
    end.


init_state(Db, Fd, #mrst{views=Views}=State, nil) ->
    Header = #mrheader{
        seq=0,
        purge_seq=couch_db:get_purge_seq(Db),
        id_btree_state=nil,
        view_states=[{nil, 0, 0} || _ <- Views]
    },
    init_state(Db, Fd, State, Header);
init_state(Db, Fd, State, Header) ->
    #mrst{language=Lang, design_opts=DOpts, views=Views} = State,
    #mrheader{
        seq=Seq,
        purge_seq=PurgeSeq,
        id_btree_state=IdBtreeState,
        view_states=ViewStates
    } = Header,

    StateUpdate = fun
        ({_, _, _}=St) -> St;
        (St) -> {St, 0, 0}
    end,
    ViewStates2 = lists:map(StateUpdate, ViewStates),

    %IdBtOpts = [{compression, couch_db:compression(Db)}],
    IdBtOpts = [],
    {ok, IdBtree} = couch_btree:open(IdBtreeState, Fd, IdBtOpts),

    OpenViewFun = fun(St, View) -> open_view(Db, Fd, Lang, DOpts, St, View) end,
    Views2 = lists:zipwith(OpenViewFun, ViewStates2, Views),

    State#mrst{
        fd=Fd,
        update_seq=Seq,
        purge_seq=PurgeSeq,
        id_btree=IdBtree,
        views=Views2
    }.


open_view(_Db, Fd, Lang, Opts, {BTState, USeq, PSeq}, View) ->
    FunSrcs = [FunSrc || {_Name, FunSrc} <- View#mrview.reduce_funs],
    ReduceFun =
        fun(reduce, KVs) ->
            KVs2 = detuple_kvs(expand_dups(KVs, []), []),
            {ok, Result} = couch_query_servers:reduce(Lang, FunSrcs, KVs2),
            {length(KVs2), Result};
        (rereduce, Reds) ->
            Count = lists:sum([Count0 || {Count0, _} <- Reds]),
            UsrReds = [UsrRedsList || {_, UsrRedsList} <- Reds],
            {ok, Result} = couch_query_servers:rereduce(Lang, FunSrcs, UsrReds),
            {Count, Result}
        end,

    case couch_util:get_value(<<"collation">>, Opts, <<"default">>) of
        <<"raw">> -> Less = fun(A,B) -> A < B end;
        _ -> Less = fun couch_view:less_json_ids/2
    end,

    ViewBtOpts = [
        {less, Less},
        {reduce, ReduceFun}
        %{compression, couch_db:compression(Db)}
    ],
    {ok, Btree} = couch_btree:open(BTState, Fd, ViewBtOpts),
    View#mrview{btree=Btree, update_seq=USeq, purge_seq=PSeq}.


make_header(State) ->
    #mrst{
        update_seq=Seq,
        purge_seq=PurgeSeq,
        id_btree=IdBtree,
        views=Views
    } = State,
    ViewStates = [
        {
            couch_btree:get_state(V#mrview.btree),
            V#mrview.update_seq,
            V#mrview.purge_seq
        }
        ||
        V <- Views
    ],
    #mrheader{
        seq=Seq,
        purge_seq=PurgeSeq,
        id_btree_state=couch_btree:get_state(IdBtree),
        view_states=ViewStates
    }.


open_index_file(RootDir, DbName, GroupSig) ->
    FName = design_root(RootDir, DbName) ++ hexsig(GroupSig) ++".view",
    case couch_file:open(FName) of
        {ok, Fd} -> {ok, Fd};
        {error, enoent} -> couch_file:open(FName, [create]);
        Error -> Error
    end.


open_compaction_file(RootDir, DbName, GroupSig) ->
    FName = design_root(RootDir, DbName) ++ hexsig(GroupSig) ++ ".compact.view",
    case couch_file:open(FName) of
        {ok, Fd} -> {ok, Fd};
        {error, enoent} -> couch_file:open(FName, [create]);
        Error -> Error
    end.


reset_index(Db, Fd, #mrst{sig=Sig}=State) ->
    ok = couch_file:truncate(Fd, 0),
    ok = couch_file:write_header(Fd, {Sig, nil}),
    init_state(Db, Fd, reset_state(State), nil).


design_root(RootDir, DbName) ->
    RootDir ++ "/." ++ binary_to_list(DbName) ++ "_design/".


reset_state(State) ->
    State#mrst{
        fd=nil,
        query_server=nil,
        update_seq=0,
        id_btree=nil,
        views=[View#mrview{btree=nil} || View <- State#mrst.views]
    }.


get_row_count(#mrview{btree=Bt}) ->
    {ok, {Count, _Reds}} = couch_btree:full_reduce(Bt),
    {ok, Count}.


reduce_to_count(Reductions) ->
    Reduce = fun
        (reduce, KVs) ->
            Counts = [
                case V of {dups, Vals} -> length(Vals); _ -> 1 end
                || {_,V} <- KVs
            ],
            {lists:sum(Counts), []};
        (rereduce, Reds) ->
            {lists:sum([Count0 || {Count0, _} <- Reds]), []}
    end,
    {Count, _} = couch_btree:final_reduce(Reduce, Reductions),
    Count.


key_opts(#mrargs{keys=[], direction=Dir}=Args) ->
    [[{dir, Dir}] ++ skey_opts(Args) ++ ekey_opts(Args)];
key_opts(#mrargs{keys=Keys}) ->
    lists:map(fun(K) ->
        [{dir, fwd}, {start_key, {K, <<>>}}, {end_key, {K, <<255>>}}]
    end, Keys).


skey_opts(#mrargs{start_key=undefined}) ->
    [];
skey_opts(#mrargs{start_key=SKey, start_key_docid=SKeyDocId}) ->
    [{start_key, {SKey, SKeyDocId}}].


ekey_opts(#mrargs{end_key=undefined}) ->
    [];
ekey_opts(#mrargs{end_key=EKey, end_key_docid=EKeyDocId}=Args) ->
    case Args#mrargs.inclusive_end of
        true -> [{end_key, {EKey, EKeyDocId}}];
        false -> [{end_key, {EKey, reverse_key_default(EKeyDocId)}}]
    end.
    

fold(#mrview{btree=Btree}, Fun, Acc, Opts) ->
    WrapperFun = fun(KV, Reds, Acc2) ->
        fold_fun(Fun, expand_dups([KV], []), Reds, Acc2)
    end,
    {ok, _LastRed, _Acc} = couch_btree:fold(Btree, WrapperFun, Acc, Opts).


fold_fun(_Fun, [], _, Acc) ->
    {ok, Acc};
fold_fun(Fun, [KV|Rest], {KVReds, Reds}, Acc) ->
    case Fun(KV, {KVReds, Reds}, Acc) of
        {ok, Acc2} ->
            fold_fun(Fun, Rest, {[KV|KVReds], Reds}, Acc2);
        {stop, Acc2} ->
            {stop, Acc2}
    end.


reverse_key_default(<<>>) -> <<255>>;
reverse_key_default(<<255>>) -> <<>>;
reverse_key_default(Key) -> Key.
    

maybe_load_doc(_, _, _, false) ->
    [];
maybe_load_doc(Db, Id, Value, _IncludeDocs) ->
    etap:diag("Include: ~p", [_IncludeDocs]),
    DocId = case Value of
        {Props} -> couch_util:get_value(<<"_id">>, Props, Id);
        _ -> Id
    end,
    case couch_db:open_doc(Db, DocId, []) of
        {not_found, missing} -> [];
        {not_found, deleted} -> [{doc, null}];
        {ok, Doc} -> [{doc, couch_doc:to_json_obj(Doc, [])}]
    end.


sort_lib({Lib}) ->
    sort_lib(Lib, []).
sort_lib([], LAcc) ->
    lists:keysort(1, LAcc);
sort_lib([{LName, {LObj}}|Rest], LAcc) ->
    LSorted = sort_lib(LObj, []), % descend into nested object
    sort_lib(Rest, [{LName, LSorted}|LAcc]);
sort_lib([{LName, LCode}|Rest], LAcc) ->
    sort_lib(Rest, [{LName, LCode}|LAcc]).


detuple_kvs([], Acc) ->
    lists:reverse(Acc);
detuple_kvs([KV | Rest], Acc) ->
    {{Key,Id},Value} = KV,
    NKV = [[Key, Id], Value],
    detuple_kvs(Rest, [NKV | Acc]).


expand_dups([], Acc) ->
    lists:reverse(Acc);
expand_dups([{Key, {dups, Vals}} | Rest], Acc) ->
    Expanded = [{Key, Val} || Val <- Vals],
    expand_dups(Rest, Expanded ++ Acc);
expand_dups([KV | Rest], Acc) ->
    expand_dups(Rest, [KV | Acc]).


hexsig(GroupSig) ->
    couch_util:to_hex(binary_to_list(GroupSig)).


index_of(Key, List) ->
    index_of(Key, List, 1).


index_of(_, [], _) ->
    throw({error, missing_named_view});
index_of(Key, [Key | _], Idx) ->
    Idx;
index_of(Key, [_ | Rest], Idx) ->
    index_of(Key, Rest, Idx+1).


mrverror(Mesg) ->
    throw({query_parse_error, Mesg}).
