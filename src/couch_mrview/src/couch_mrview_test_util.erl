-module(couch_mrview_test_util).

-compile(export_all).

-include("couch_db.hrl").
-define(ADMIN, #user_ctx{roles=[<<"_admin">>]}).


init_db(Name) ->
    couch_server:delete(Name, [{user_ctx, ?ADMIN}]),
    {ok, Db} = couch_db:create(Name, [{user_ctx, ?ADMIN}]),
    Docs = [ddoc()] ++ make_docs(10),
    {ok, _} = couch_db:update_docs(Db, Docs, []),
    couch_db:reopen(Db).


make_docs(Count) ->
    make_docs(Count, []).

make_docs(Count, Acc) when Count =< 0 ->
    Acc;
make_docs(Count, Acc) ->
    make_docs(Count-1, [doc(Count) | Acc]).


ddoc() ->
    couch_doc:from_json_obj({[
        {<<"_id">>, <<"_design/bar">>},
        {<<"views">>, {[
            {<<"baz">>, {[
                {<<"map">>, <<"function(doc) {emit(doc.val, doc.val);}">>}
            ]}}
        ]}}
    ]}).


doc(Id) ->
    couch_doc:from_json_obj({[
        {<<"_id">>, list_to_binary(integer_to_list(Id))},
        {<<"val">>, Id}
    ]}).
