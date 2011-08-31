-module(couch_mrview_cleanup).

-export([run/1]).


-include("couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").


run(Db) ->
    IdxDir = couch_config:get("couchdb", "index_dir"),
    DbName = couch_db:name(Db),
    DbNameL = binary_to_list(DbName),

    {ok, DesignDocs} = couch_db:get_design_docs(Db),
    SigFiles = lists:foldl(fun(DDoc, SFAcc) ->
        {ok, InitState} = couch_mrview_util:ddoc_to_mrst(DbName, DDoc),
        Sig = InitState#mrst.sig,
        IFName = couch_mrview_util:index_file(IdxDir, DbName, Sig),
        CFName = couch_mrview_util:compaction_file(IdxDir, DbName, Sig),
        [IFName, CFName | SFAcc]
    end, [], [DD || DD <- DesignDocs, DD#doc.deleted == false]),
   
    BaseDir = IdxDir ++ "/." ++ DbNameL ++ "_design/mrview/*",
    DiskFiles = filelib:wildcard(BaseDir),

    % We need to delete files that have no ddoc.
    ToDelete = DiskFiles -- SigFiles,
    
    lists:foreach(fun(FN) ->
        ?LOG_DEBUG("Deleting stale view file: ~s", [FN]),
        couch_file:delete(IdxDir, FN, false)
    end, ToDelete),
    
    ok.
