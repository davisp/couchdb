#include "erl_nif.h"

typedef ERL_NIF_TERM ENTERM;
typedef const ERL_NIF_TERM CENTERM;

typedef struct
{
    ErlNifResourceType* res_type;
} state_t;

typedef struct
{
    ErlNifEnv*  env;
    ENTERM      ref;
} ptr_t;


static void
ptr_destroy(ErlNifEnv* env, void* obj)
{
    ptr_t* ptr = (ptr_t*) obj;
    enif_free_env(ptr->env);
}

static int
load(ErlNifEnv* env, void** priv, ENTERM load_info)
{
    ErlNifResourceType* res;
    state_t* state = (state_t*) enif_alloc(sizeof(state_t));
    const char* name = "Context";
    int flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;

    if(state == NULL) goto error;

    state->res_type = NULL;

    res = enif_open_resource_type(env, NULL, name, ptr_destroy, flags, NULL);
    if(res == NULL) goto error;
    state->res_type = res;

    *priv = (void*) state;
    
    return 0;

error:
    if(state != NULL) enif_free(state);
    return -1;
}

static void
unload(ErlNifEnv* env, void* priv)
{
    state_t* state = (state_t*) priv;
    enif_free(state);
}

static ENTERM
wrap(ErlNifEnv* env, int argc, CENTERM argv[])
{
    state_t* state = (state_t*) enif_priv_data(env);
    ptr_t* ptr = (ptr_t*) enif_alloc_resource(state->res_type, sizeof(ptr_t));
    ENTERM ret;
    
    ptr->env = enif_alloc_env();
    ptr->ref = enif_make_copy(ptr->env, argv[0]);
    
    ret = enif_make_resource(env, ptr);
    enif_release_resource(ptr);

    return ret;
}

static ENTERM
unwrap(ErlNifEnv* env, int argc, CENTERM argv[])
{
    state_t* state = (state_t*) enif_priv_data(env);
    ptr_t* ptr = NULL;

    if(!enif_get_resource(env, argv[0], state->res_type, &ptr))
    {
        return enif_make_badarg(env);
    }

    return enif_make_copy(env, ptr->ref);
}

static ErlNifFunc nif_funcs[] = {
    {"wrap", 1, wrap},
    {"unwrap", 1, unwrap}
};

ERL_NIF_INIT(erlptr, nif_funcs, load, NULL, NULL, unload);

