-module(couch_mrview).

-export([query_view/3, query_view/4, query_view/6, get_info/2]).

-include_lib("couch_mrview/include/couch_mrview.hrl").

-record(mracc, {
    db,
    total_rows,
    offset,
    limit,
    skip,
    group_level,
    callback,
    user_acc,
    reduce_fun,
    include_docs
}).


query_view(Db, DDoc, ViewName) ->
    query_view(Db, DDoc, ViewName, #mrargs{}).


query_view(Db, DDoc, ViewName, Args) when is_list(Args) ->
    query_view(Db, DDoc, ViewName, to_mrargs(Args));
query_view(Db, DDoc, ViewName, Args) ->
    query_view(Db, DDoc, ViewName, Args, fun default_callback/2, []).


query_view(Db, DDoc, ViewName, Args0, Callback, Acc0) when is_list(Args0) ->
    query_view(Db, DDoc, ViewName, to_mrargs(Args0), Callback, Acc0);
query_view(Db, DDoc, ViewName, Args0, Callback, Acc0) ->
    {Type, View, Args} = couch_mrview_util:get_view(Db, DDoc, ViewName, Args0),
    case Type of
        map -> map_fold(Db, View, Args, Callback, Acc0);
        red -> red_fold(Db, View, Args, Callback, Acc0)
    end.


% API convenience.
get_info(Db, DDoc) ->
    couch_mrview_util:get_info(Db, DDoc).


map_fold(Db, View, Args, Callback, UAcc) ->
    {ok, Total} = couch_mrview_util:get_row_count(View),
    Acc = #mracc{
        db=Db,
        total_rows=Total,
        limit=Args#mrargs.limit,
        skip=Args#mrargs.skip,
        callback=Callback,
        user_acc=UAcc,
        reduce_fun=fun couch_mrview_util:reduce_to_count/1,
        include_docs=Args#mrargs.include_docs
    },
    OptList = couch_mrview_util:key_opts(Args),
    Acc2 = lists:foldl(fun(Opts, Acc0) ->
        {ok, _, Acc1} = couch_mrview_util:fold(View, fun map_fold/3, Acc0,Opts),
        Acc1
    end, Acc, OptList),
    {_, UAcc1} = Callback(complete, Acc2#mracc.user_acc),
    {ok, UAcc1}.


map_fold(_KV, _Offset, #mracc{skip=N}=Acc) when N > 0 ->
    {ok, Acc#mracc{skip=N-1}};
map_fold(KV, OffsetReds, #mracc{offset=undefined}=Acc) ->
    #mracc{
        total_rows=Total,
        callback=Callback,
        user_acc=UAcc0,
        reduce_fun=Reduce
    } = Acc,
    Offset = Reduce(OffsetReds),
    {Go, UAcc1} = Callback({total_and_offset, Total, Offset}, UAcc0),
    Acc1 = Acc#mracc{offset=Offset, user_acc=UAcc1},
    case Go of
        ok -> map_fold(KV, OffsetReds, Acc1);
        stop -> {stop, Acc1}
    end;
map_fold(_KV, _Offset, #mracc{limit=0}=Acc) ->
    {stop, Acc};
map_fold({{Key, Id}, Val}, _Offset, Acc) ->
    #mracc{
        db=Db,
        limit=Limit,
        callback=Callback,
        user_acc=UAcc0,
        include_docs=IncludeDocs
    } = Acc,
    Doc = couch_mrview_util:maybe_load_doc(Db, Id, Val, IncludeDocs),
    Row = [{key, Key}, {id, Id}, {val, Val}] ++ Doc,
    {Go, UAcc1} = Callback({row, Row}, UAcc0),
    {Go, Acc#mracc{limit=Limit-1, user_acc=UAcc1}}.


red_fold(Db, View, Args, Callback, UAcc) ->
    Acc = #mracc{
        db=Db,
        total_rows=null,
        limit=Args#mrargs.limit,
        skip=Args#mrargs.skip,
        group_level=Args#mrargs.group_level,
        callback=Callback,
        user_acc=UAcc,
        include_docs=Args#mrargs.include_docs
    },
    GroupFun = group_rows_fun(Args#mrargs.group_level),
    OptList = couch_mrview_util:key_opts(Args, [{key_group_fun, GroupFun}]),
    Acc2 = lists:foldl(fun(Opts, Acc0) ->
        {ok, Acc1} =
            couch_mrview_util:fold_reduce(View, fun red_fold/3,  Acc0, Opts),
        Acc1
    end, Acc, OptList),
    {_, UAcc1} = Callback(complete, Acc2#mracc.user_acc),
    {ok, UAcc1}.


red_fold(_Key, _Red, #mracc{skip=N}=Acc) when N > 0 ->
    {ok, Acc#mracc{skip=N-1}};
red_fold(_Key, _Red, #mracc{limit=0} = Acc) ->
    {stop, Acc};
red_fold(_Key, Red, #mracc{group_level=0} = Acc) ->
    #mracc{
        limit=Limit,
        callback=Callback,
        user_acc=UAcc0
    } = Acc,
    Row = [{key, null}, {val, Red}],
    {Go, UAcc1} = Callback({row, Row}, UAcc0),
    {Go, Acc#mracc{user_acc=UAcc1, limit=Limit-1}};
red_fold(Key, Red, #mracc{group_level=exact} = Acc) ->
    #mracc{
        limit=Limit,
        callback=Callback,
        user_acc=UAcc0
    } = Acc,
    Row = [{key, Key}, {val, Red}],
    {Go, UAcc1} = Callback({row, Row}, UAcc0),
    {Go, Acc#mracc{user_acc=UAcc1, limit=Limit-1}};
red_fold(K, Red, #mracc{group_level=I} = Acc) when I > 0, is_list(K) ->
    #mracc{
        limit=Limit,
        callback=Callback,
        user_acc=UAcc0
    } = Acc,
    Row = [{key, lists:sublist(K, I)}, {val, Red}],
    {Go, UAcc1} = Callback({row, Row}, UAcc0),
    {Go, Acc#mracc{user_acc=UAcc1, limit=Limit-1}};
red_fold(K, Red, #mracc{group_level=I} = Acc) when I > 0 ->
    #mracc{
        limit=Limit,
        callback=Callback,
        user_acc=UAcc0
    } = Acc,
    Row = [{key, K}, {val, Red}],
    {Go, UAcc1} = Callback({row, Row}, UAcc0),
    {Go, Acc#mracc{user_acc=UAcc1, limit=Limit-1}}.


group_rows_fun(exact) ->
    fun({Key1,_}, {Key2,_}) -> Key1 == Key2 end;
group_rows_fun(0) ->
    fun(_A, _B) -> true end;
group_rows_fun(GroupLevel) when is_integer(GroupLevel) ->
    fun({[_|_] = Key1,_}, {[_|_] = Key2,_}) ->
        lists:sublist(Key1, GroupLevel) == lists:sublist(Key2, GroupLevel);
    ({Key1,_}, {Key2,_}) ->
        Key1 == Key2
    end.


default_callback(complete, Acc) ->
    {ok, lists:reverse(Acc)};
default_callback(Row, Acc) ->
    {ok, [Row | Acc]}.


to_mrargs(KeyList) ->
    lists:foldl(fun({Key, Value}, Acc) ->
        Index = lookup_index(couch_util:to_existing_atom(Key)),
        setelement(Index, Acc, Value)
    end, #mrargs{}, KeyList).


lookup_index(Key) ->
    Index = lists:zip(
        record_info(fields, mrargs), lists:seq(2, record_info(size, mrargs))
    ),
    couch_util:get_value(Key, Index).
