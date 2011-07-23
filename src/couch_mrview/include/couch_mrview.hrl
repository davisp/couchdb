-record(group, {
    sig=nil,
    fd=nil,
    name,
    def_lang,
    design_options=[],
    views,
    lib,
    id_btree=nil,
    current_seq=0,
    purge_seq=0,
    query_server=nil,
    waiting_delayed_commit=nil
    }).

-record(view,
    {id_num,
    update_seq=0,
    purge_seq=0,
    map_names=[],
    def,
    btree=nil,
    reduce_funs=[],
    options=[]
    }).

-record(index_header,
    {seq=0,
    purge_seq=0,
    id_btree_state=nil,
    view_states=nil
    }).