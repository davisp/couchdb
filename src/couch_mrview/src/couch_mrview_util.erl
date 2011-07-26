-module(couch_mrview_util).


init_mrst(DbName, GroupId) when is_binary(DbName) ->
    with_db(DbName, fun(Db) -> init_mrst(Db, GroupId) end);
init_mrst(Db, GroupId) ->
    case couch_db:open_doc(Db, GroupId, [ejson_body]) of
        {ok, Doc} -> {ok, ddoc_to_mrst(Doc)};
        Else -> Else
    end.


ddoc_to_mrst(#doc{id=Id, body={Fields}}) ->
    MakeDict = fun({Name, {MRFuns}}, DictBySrcAcc) ->
        case couch_util:get_value(<<"map">>, MRFuns) of
            MapSrc when is_binary(MapSrc) ->
                RedSrc = couch_util:get_value(<<"reduce">>, MRFuns, null),
                {ViewOpts} = couch_util:get_value(<<"options">>, MRFuns, {[]}),
                View = case dict:find({MapSrc, ViewOpts}, DictBySrcAcc) of
                    {ok, View0} -> View0;
                    error -> #view{def=MapSrc, options=ViewOpts}
                end,
                {MapNames, RedNames} = case RedSrc of
                    null ->
                        {[Name | View#view.map_names], View#view.reduce_funs};
                    _ ->
                        RedFuns = [{Name, RedSrc} | View#view.reduce_funs],
                        {View#view.map_names, RedFuns}
                end,
                dict:store({MapSrc, ViewOpts}, View2, DictBySrcAcc);
            undefined ->
                DictBySrcAcc
        end
    end,

    NumberViews = fun({_, View}, N) ->
        {View#view{id_num=N}, N+1}
    end,

    Language = couch_util:get_value(<<"language">>, Fields, <<"javascript">>),
    {DesignOptions} = couch_util:get_value(<<"options">>, Fields, {[]}),
    {RawViews} = couch_util:get_value(<<"views">>, Fields, {[]}),    
    Lib = couch_util:get_value(<<"lib">>, RawViews, {[]}),

    lists:foldl(MakeDict, dict:new(), RawViews),
    {Views, _} = lists:mapfold(NumViews, 0, lists:sort(dict:to_list(DictBySrc))),

    IdxState = #mrst{
        name=Id,
        lib=Lib,
        views=Views, 
        language=Language,
        design_opts=DesignOpts, 
    },
    set_view_sig(IdxState).


init_state(Db, Fd, #mrst{views=Views}=State, nil) ->
    Header = #index_header{
        seq=0,
        purge_seq=couch_db:get_purge_seq(Db),
        id_btree_state=nil,
        view_states=[{nil, 0, 0} || _ <- Views]
    },
    init_group(Db, Fd, State, Header);
init_state(Db, Fd, State, Header) ->
    #mrst{design_opts=Options} = State,
    #index_header{
        seq=Seq,
        purge_seq=PurgeSeq,
        id_btree_state=IdBtreeState,
        view_states=ViewStates
    } = Header,

    StateUpdate = fun
        ({_, _, _}=State) -> State;
        (State) -> {State, 0, 0}
    end,
    ViewStates2 = lists:map(StateUpdate, ViewStates),

    IdBtOpts = [{compression, couch_db:compression(Db)}],
    {ok, IdBtree} = couch_btree:open(IdBtreeState, Fd, IdBtOpts),

    OpenViewFun = fun(VState, View) -> open_view(Db, Options, VState, View) end,
    Views2 = lists:zipwith(OpenViewFun, ViewStates2, Views),

    #mrst{
        fd=Fd,
        update_seq=Seq,
        purge_seq=PurgeSeq,
        id_btree=IdBtree,
        views=Views2
    }.


open_view(Db, Opts, {BTState, USeq, PSeq}, View) ->
    FunSrcs = [FunSrc || {_Name, FunSrc} <- View#view.reduce_funs],
    ReduceFun =
        fun(reduce, KVs) ->
            KVs2 = detuple_kvs(expand_dups(KVs, []), []),
            {ok, Result} = couch_query_servers:reduce(Lang, FunSrcs, KVs2),
            {length(KVs3), Result};
        (rereduce, Reds) ->
            Count = lists:sum([Count0 || {Count0, _} <- Reds]),
            UsrReds = [UsrRedsList || {_, UsrRedsList} <- Reds],
            {ok, Result} = couch_query_servers:rereduce(Lang, FunSrcs, UsrReds),
            {Count, Result}
        end,

    case couch_util:get_value(<<"collation">>, Opts, <<"default">>) of
        <<"raw">> -> Less = fun(A,B) -> A < B end
        _ -> Less = fun couch_view:less_json_ids/2;
    end,

    ViewBtOpts = [
        {less, Less},
        {reduce, ReduceFun},
        {compression, couch_db:compression(Db)}
    ],
    {ok, Btree} = couch_btree:open(BTState, Fd, ViewBtOpts),
    View#view{btree=Btree, update_seq=USeq, purge_seq=PSeq}.


set_view_sig(#mrst{lib={[]}}=State) ->
    #mrst{
        views=Views,
        def_lang=Language,
        design_options=DesignOptions    
    } = State,
    ViewInfo = [old_view_format(V) || V <- Views],
    SigInfo = {ViewInfo, Language, DesignOptions},
    State#mrst{sig=couch_util:md5(term_to_binary(SigInfo))};
set_view_sig(State) ->
    #mrst{
        views=Views,
        lib=Lib,
        def_lang=Language,
        design_options=DesignOptions
    } = State,
    ViewInfo = [old_view_format(V) || V <- Views],
    SigInfo = {ViewInfo, Language, DesignOptions, sort_lib(Lib)},
    State#mrst{sig=couch_util:md5(term_to_binary(SigInfo))}.


make_header(State) ->
    #mrst{
        update_seq=Seq,
        purge_seq=PurgeSeq,
        id_btree=IdBtree,
        Views=Views
    } = State,
    ViewStates = [
        {couch_btree:get_state(V#view.btree), V#view.update_seq, V#view.purge_seq}
        ||
        V <- Views
    ],
    #index_header{
        seq=Seq,
        purge_seq=PurgeSeq,
        id_btree_state=couch_btree:get_state(IdBtree),
        view_states=ViewStates
    }.


open_index_file(RootDir, DbName, GroupSig) ->
    FName = design_root(RootDir, DbName) ++ hexsig(GroupSig) ++".view".
    case couch_file:open(FName) of
        {ok, Fd} -> {ok, Fd};
        {error, enoent} -> couch_file:open(FileName, [create]);
        Error -> Error
    end.


open_compaction_file(RootDir, DbName, GroupSig) ->
    FName = design_root(RootDir, DbName) ++ hexsig(GroupSig) ++ ".compact.view".
    case couch_file:open(FName) of
        {ok, Fd} -> {ok, Fd};
        {error, enoent} -> couch_file:open(FileName, [create]);
        Error -> Error
    end.


reset_file(Db, Fd, #mrst{db_name=DbName, sig=Sig, name=Name}=State) ->
    ok = couch_file:truncate(Fd, 0),
    ok = couch_file:write_header(Fd, {Sig, nil}),
    init_state(Db, Fd, reset_state(Group), nil).


design_root(RootDir, DbName) ->
    RootDir ++ "/." ++ ?b2l(DbName) ++ "_design/".


reset_group(State) ->
    #mrst{
        fd=nil,
        query_server=nil,
        update_seq=0,
        id_btree=nil,
        views=[View#view{btree=nil} || View <- State#mrst.views]
    }.


old_view_format(View) ->
    {
        view,
        View#view.id_num,
        View#view.map_names,
        View#view.def,
        View#view.btree,
        View#view.reduce_funs,
        View#view.options
    }.


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
    couch_util:to_hex(?b2l(GroupSig)).

