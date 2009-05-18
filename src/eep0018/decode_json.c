/* Copyright (c) 2008-2009 Paul J. Davis <paul.joseph.davis@gmail.com>
 * Copyright (c) 2008-2009 Enrico Thierbach <eno@open-lab.org>
 *
 * This file is part of EEP0018, which is released under the MIT license.
 */

#ifndef WIN32
#include <string.h>
#endif

#include "eep0018.h"
#include "yajl_parse.h"

#define INIT_DBL_LEN 1024
#define INIT_TERM_LEN 4096
#define INIT_OBJ_LEN 256
#define INIT_STR_LEN 4096

#define NO_TYPE 0
#define MAP_TYPE 1
#define ARRAY_TYPE 2

#define CHECK(call) if((call) != OK) goto done

typedef ErlDrvTermData TermData;
typedef unsigned char uchar;

typedef struct
{
    double*         dbl_data;
    int             dbl_length;
    int             dbl_used;

    TermData*       term_data;
    int             term_length;
    int             term_used;

    char*           obj_types;
    int*            obj_members;
    int             obj_length;
    int             obj_used;

    uchar*          str_start;
    uchar*          str_end;
    uchar*          str_data;
    int             str_length;
    int             str_used;

    TermData        nullTerm;
    TermData        trueTerm;
    TermData        falseTerm;
} Decoder;

void destroy_decoder(Decoder* dec);

Decoder*
init_decoder(uchar* str_start, uchar* str_end)
{
    int status = ERROR;
    Decoder* dec = (Decoder*) malloc(sizeof(Decoder));
    if(dec == NULL) goto done;
    memset(dec, 0, sizeof(Decoder));

    dec->dbl_data = (double*) malloc(INIT_DBL_LEN * sizeof(double));
    if(dec->dbl_data == NULL) goto done;
    dec->dbl_length = INIT_DBL_LEN;
    dec->dbl_used = 0;

    dec->term_data = (TermData*) malloc(INIT_TERM_LEN * sizeof(TermData));
    if(dec->term_data == NULL) goto done;
    dec->term_length = INIT_TERM_LEN;
    dec->term_used = 0;

    dec->obj_types = (char*) malloc(INIT_OBJ_LEN * sizeof(char));
    if(dec->obj_types == NULL) goto done;
    dec->obj_members = (int*) malloc(INIT_OBJ_LEN * sizeof(int));
    if(dec->obj_members == NULL) goto done;
    dec->obj_length = INIT_OBJ_LEN;
    dec->obj_used = 0;

    dec->str_start = str_start;
    dec->str_end = str_end;
    dec->str_data = (uchar*) malloc(INIT_STR_LEN * sizeof(uchar));
    if(dec->str_data == NULL) goto done;
    dec->str_length = INIT_STR_LEN;
    dec->str_used = 0;

    dec->nullTerm = driver_mk_atom("null");
    dec->trueTerm = driver_mk_atom("true");
    dec->falseTerm = driver_mk_atom("false");

    status = OK;

done:
    if(status != OK && dec != NULL)
    {
        destroy_decoder(dec);
        dec = NULL;
    }

    return dec;
}

void
destroy_decoder(Decoder* dec)
{
    if(dec == NULL) return;
    if(dec->dbl_data != NULL) free(dec->dbl_data);
    if(dec->term_data != NULL) free(dec->term_data);
    if(dec->obj_types != NULL) free(dec->obj_types);
    if(dec->obj_members != NULL) free(dec->obj_members);
    if(dec->str_data != NULL) free(dec->str_data);
    free(dec);
}

static inline double*
add_double(Decoder* dec, double val)
{
    double* ret = NULL;
    double* next = NULL;

    if(dec->dbl_used >= dec->dbl_length)
    {
        next = (double*) malloc(dec->dbl_length * 2 * sizeof(double));
        if(next == NULL) goto done;
        
        memcpy(next, dec->dbl_data, dec->dbl_length * sizeof(double));
        free(dec->dbl_data);
        dec->dbl_data = next;

        dec->dbl_length *= 2;
    }

    dec->dbl_data[dec->dbl_used++] = val;
    ret = dec->dbl_data + (dec->dbl_used-1);

done:
    return ret;
}

static inline int
add_term(Decoder* dec, TermData val)
{
    int ret = ERROR;
    TermData* next = NULL;

    if(dec->term_used >= dec->term_length)
    {
        next = (TermData*) malloc(dec->term_length * 2 * sizeof(TermData));
        if(next == NULL) goto done;

        memcpy(next, dec->term_data, dec->term_length * sizeof(TermData));
        free(dec->term_data);
        dec->term_data = next;

        dec->term_length *= 2;
    }

    dec->term_data[dec->term_used++] = val;
    ret = OK;

done:
    return ret;
}

static inline int
open_object(Decoder* dec, char type)
{
    int ret = ERROR;
    char* next_typ = NULL;
    int* next_mem = NULL;

    if(dec->obj_used >= dec->obj_length)
    {
        next_typ = (char*) malloc(dec->obj_length * 2 * sizeof(char));
        if(next_typ == NULL) goto done;
        next_mem = (int*) malloc(dec->obj_length * 2 * sizeof(int));
        if(next_mem == NULL) goto done;

        memcpy(next_typ, dec->obj_types, dec->obj_length * sizeof(char));
        free(dec->obj_types);
        dec->obj_types = next_typ;
        next_typ = NULL;

        memcpy(next_mem, dec->obj_members, dec->obj_length * sizeof(int));
        free(dec->obj_members);
        dec->obj_members = next_mem;
        next_mem = NULL;

        dec->obj_length *= 2;
    }

    dec->obj_types[dec->obj_used] = type;
    dec->obj_members[dec->obj_used] = 0;
    dec->obj_used++;
    ret = OK;

done:
    if(ret != OK && next_typ != NULL) free(next_typ);
    if(ret != OK && next_mem != NULL) free(next_mem);
    return ret;
}

static inline int
check_object(Decoder* dec)
{
    if(dec->obj_used == 0) return OK;
    
    // Adding a member to this object.
    dec->obj_members[dec->obj_used-1]++;

    if(dec->obj_types[dec->obj_used-1] != MAP_TYPE) return OK;

    //fprintf(stderr, "Adding map tuple\n");
    if(!add_term(dec, ERL_DRV_TUPLE)) return ERROR;
    return add_term(dec, 2);
}

static inline int
close_object(Decoder* dec)
{
    int ret = ERROR;

    if(dec->obj_used < 1) goto done;

    if(dec->obj_types[dec->obj_used-1] == MAP_TYPE)
    {
        // {"foo": 1} -> {[{<<"foo">>, 1}]}
        // Close proplist
        CHECK(add_term(dec, ERL_DRV_NIL));
        CHECK(add_term(dec, ERL_DRV_LIST));
        CHECK(add_term(dec, dec->obj_members[dec->obj_used-1]+1));
        
        // Close tuple
        CHECK(add_term(dec, ERL_DRV_TUPLE));
        CHECK(add_term(dec, 1));
    }
    else if(dec->obj_types[dec->obj_used-1] == ARRAY_TYPE)
    {
        // Close list
        CHECK(add_term(dec, ERL_DRV_NIL));
        CHECK(add_term(dec, ERL_DRV_LIST));
        CHECK(add_term(dec, dec->obj_members[dec->obj_used-1]+1));
    }
    else
    {
        // Invalid object type.
        goto done;
    }

    dec->obj_types[dec->obj_used] = NO_TYPE;
    dec->obj_members[dec->obj_used] = 0;
    dec->obj_used--;
    ret = OK;

done:
    return ret;
}

static inline const uchar*
add_string(Decoder* dec, const uchar* buf, unsigned int length)
{
    const uchar* ret = NULL;
    uchar* next = NULL;
    int new_length = 0;

    // String still in buffer
    if(buf >= dec->str_start && buf <= dec->str_end) return buf;
    
    if(length > dec->str_length - dec->str_used)
    {
        new_length = 2 * dec->str_length;
        if(length > new_length - dec->str_used) new_length += 2 * length;
        next = (uchar*) malloc(new_length * sizeof(uchar));
        if(next == NULL) goto done;

        memcpy(next, dec->str_data, dec->str_length * sizeof(uchar));
        free(dec->str_data);
        dec->str_data = next;

        dec->str_length = new_length;
    }

    memcpy(dec->str_data + dec->str_used, buf, length * sizeof(uchar));
    dec->str_used += length;
    ret = dec->str_data + (dec->str_used - length);
done:
    return ret;
}

static inline int
json_null(void* ctx)
{
    //fprintf(stderr, "null\n");
    Decoder* dec = (Decoder*) ctx;
    if(add_term(dec, ERL_DRV_ATOM) != OK) return ERROR;
    if(add_term(dec, dec->nullTerm) != OK) return ERROR;
    return check_object(dec);
}

static inline int
json_boolean(void* ctx, int boolVal)
{
    //fprintf(stderr, "boolean\n");
    Decoder* dec = (Decoder*) ctx;
    if(boolVal)
    {
        if(add_term(dec, ERL_DRV_ATOM) != OK) return ERROR;
        if(add_term(dec, dec->trueTerm) != OK) return ERROR;
    }
    else
    {
        if(add_term(dec, ERL_DRV_ATOM) != OK) return ERROR;
        if(add_term(dec, dec->falseTerm) != OK) return ERROR;
    }

    return check_object(dec);
}

static inline int
json_integer(void* ctx, long integerVal)
{
    //fprintf(stderr, "integer\n");
    Decoder* dec = (Decoder*) ctx;
    if(add_term(dec, ERL_DRV_INT) != OK) return ERROR;
    if(add_term(dec, (ErlDrvSInt) integerVal) != OK) return ERROR;
    return check_object(dec);
}

static inline int
json_double(void* ctx, double doubleVal)
{
    //fprintf(stderr, "double\n");
    Decoder* dec = (Decoder*) ctx;
    double* pos = add_double(dec, doubleVal);
    if(pos == NULL) return ERROR;

    if(add_term(dec, ERL_DRV_FLOAT) != OK) return ERROR;
    if(add_term(dec, (TermData) pos) != OK) return ERROR;
    return check_object(dec);
}

static inline int
json_string(void* ctx, const unsigned char* stringVal, unsigned int stringLen)
{
    //fprintf(stderr, "string\n");
    Decoder* dec = (Decoder*) ctx;
    const uchar* data = add_string(dec, stringVal, stringLen);
    if(add_term(dec, ERL_DRV_BUF2BINARY) != OK) return ERROR;
    if(add_term(dec, (ErlDrvTermData) data) != OK) return ERROR;
    if(add_term(dec, stringLen) != OK) return ERROR;
    return check_object(dec);
}

static inline int
json_start_map(void* ctx)
{
    //fprintf(stderr, "map start\n");
    Decoder* dec = (Decoder*) ctx;
    return open_object(dec, MAP_TYPE);
}

static inline int
json_map_key(void* ctx, const unsigned char* key, unsigned int stringLen)
{
    //fprintf(stderr, "map key\n");
    Decoder* dec = (Decoder*) ctx;
    const uchar* buf = add_string(dec, key, stringLen);
    if(key == NULL) return ERROR;
    if(add_term(dec, ERL_DRV_BUF2BINARY) != OK) return ERROR;
    if(add_term(dec, (ErlDrvTermData) buf) != OK) return ERROR;
    return add_term(dec, stringLen);
}

static inline int
json_end_map(void* ctx)
{
    //fprintf(stderr, "map end\n");
    Decoder* dec = (Decoder*) ctx;
    if(close_object(dec) != OK) return ERROR;
    return check_object(dec);
}

static inline int
json_start_array(void* ctx)
{
    //fprintf(stderr, "array start\n");
    Decoder* dec = (Decoder*) ctx;
    return open_object(dec, ARRAY_TYPE);
}

static inline int
json_end_array(void* ctx)
{
    //fprintf(stderr, "array end\n");
    Decoder* dec = (Decoder*) ctx;
    if(close_object(dec) != OK) return ERROR;
    return check_object(dec);
}

static yajl_callbacks json_callbacks = {
    json_null,
    json_boolean,
    json_integer,
    json_double,
    NULL,
    json_string,
    json_start_map,
    json_map_key,
    json_end_map,
    json_start_array,
    json_end_array
};

int
decode_json(ErlDrvPort port, char* buf, int len, char** rbuf, int rlen)
{
    unsigned char* ubuf = (unsigned char*) buf;
    int ret = -1;
    int resp;
    uchar* mesg = NULL;
    int mlen = 0;
   
    Decoder* dec = init_decoder(ubuf, ubuf+len);
    if(dec == NULL) goto done;

    // Setup so we return {ok, Json}
    if(add_term(dec, ERL_DRV_ATOM) != OK) goto done;
    if(add_term(dec, driver_mk_atom("json")) != OK) goto done;

    yajl_parser_config conf = {0, 1}; // No comments, check utf8
    yajl_handle handle = yajl_alloc(&json_callbacks, &conf, NULL, dec);
    yajl_status status = yajl_parse(handle, (unsigned char*) buf, len);

    if(status != yajl_status_ok)
    {
        mesg = yajl_get_error(handle, 0, NULL, 0);
        mlen = strlen((char*) mesg);
        memcpy(*rbuf, mesg, mlen < rlen ? mlen : rlen);
        ret = mlen < rlen ? mlen : rlen;
        goto done;
    }

    // Finish the tuple of the our response
    if(add_term(dec, ERL_DRV_TUPLE) != OK) goto done;
    if(add_term(dec, 2) != OK) goto done;

    *rbuf = NULL;
    resp = driver_send_term(
        port,
        driver_caller(port),
        dec->term_data,
        dec->term_used
    );
    if(resp == 1) ret = 0;

done:
    if(mesg != NULL) yajl_free_error(handle, mesg);
    if(handle != NULL) yajl_free(handle);
    destroy_decoder(dec);
    return ret;
}
