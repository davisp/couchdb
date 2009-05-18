/* Copyright (c) 2008-2009 Paul J. Davis <paul.joseph.davis@gmail.com>
 * Copyright (c) 2008-2009 Enrico Thierbach <eno@open-lab.org>
 *
 * This file is part of EEP0018, which is released under the MIT license.
 */

#include <ei.h>

#ifndef WIN32
#include <string.h>
#endif

#include "eep0018.h"
#include "yajl_gen.h"

#ifdef VERSION
#undef VERSION
#endif

#define LONG        97
#define NEG_LONG    98
#define DOUBLE      99
#define ATOM        100
#define TUPLE       104
#define EMPTY_LIST  106
#define STRING      107
#define LIST        108
#define BINARY      109
#define VERSION     131

int
atom_to_json(char* buf, int* index, yajl_gen handle)
{
    char data[MAXATOMLEN+1];
    unsigned char* atom = (unsigned char*) data;
    int type, size;
    int ret = ERROR;

    // Check atom type?
    if(ei_get_type(buf, index, &type, &size)) goto done;
    if(type != ATOM) goto done;
    if(ei_decode_atom(buf, index, data)) goto done;

    if(strcmp(data, "null") == 0)
    {
        if(yajl_gen_null(handle) != yajl_gen_status_ok) goto done;
    }
    else if(strcmp(data, "true") == 0)
    {
        if(yajl_gen_bool(handle, 1) != yajl_gen_status_ok) goto done;
    }
    else if(strcmp(data, "false") == 0)
    {
        if(yajl_gen_bool(handle, 0) != yajl_gen_status_ok) goto done;
    }
    else
    {
        if(yajl_gen_string(handle, atom, strlen(data)) != yajl_gen_status_ok)
        {
            goto done;
        }
    }

    ret = OK;

done:
    return ret;
}

int
string_to_json(char* buf, int* index, yajl_gen handle)
{
    int ret = ERROR;
    int i, type, size, val;

    if(ei_get_type(buf, index, &type, &size)) goto done;
    if(type != STRING) goto done;

    *index += 3;

    if(yajl_gen_array_open(handle) != yajl_gen_status_ok) goto done;
    for(i = 0; i < size; i++)
    {
        val = (int) *(buf+*index+i);
        if(yajl_gen_integer(handle, val) != yajl_gen_status_ok) goto done;
    }
    if(yajl_gen_array_close(handle) != yajl_gen_status_ok) goto done;

    *index += size;
    ret = OK;

done:
    return ret;
}

int
key_to_json(char* buf, int* index, yajl_gen handle)
{
    int ret = ERROR;
    char data[MAXATOMLEN];
    int type, size;
    unsigned char* string;

    if(ei_get_type(buf, index, &type, &size)) goto done;
    if(type == BINARY)
    {
        return binary_to_json(buf, index, handle);
    }
    else if(type == ATOM)
    {
        if(ei_decode_atom(buf, index, data)) goto done;
        string = (unsigned char*) data;
        if(yajl_gen_string(handle, string, size) != yajl_gen_status_ok)
        {
            goto done;
        }
    }
    else if(type == STRING)
    {
        *index += 3;
        string = (unsigned char*) (buf + *index);
        if(yajl_gen_string(handle, string, size) != yajl_gen_status_ok)
        {
            goto done;
        }
        *index += size;
    }
    else
    {
        // Invalid key type.
        goto done;
    }

    ret = OK;

done:
    return ret;
}

int
map_to_json(char* buf, int* index, yajl_gen handle)
{
    char* key = NULL;
    int ret = ERROR;
    int i, arity, size;

    // {[{<<"foo">>, 1}]} -> {"foo": 1}
    if(ei_decode_tuple_header(buf, index, &arity)) goto done;
    if(arity != 1) goto done;

    if(ei_decode_list_header(buf, index, &arity)) goto done;

    if(yajl_gen_map_open(handle) != yajl_gen_status_ok) goto done;

    for(i = 0 ; i < arity ; i++)
    {
        if(ei_decode_tuple_header(buf, index, &size)) goto done;
        if(size != 2) goto done;
        if(key_to_json(buf, index, handle) != OK) goto done;
        if(value_to_json(buf, index, handle) != OK) goto done;
    }

    if(yajl_gen_map_close(handle) != yajl_gen_status_ok) goto done;

    if(arity > 0)
    {
        if(ei_decode_list_header(buf, index, &arity)) goto done;
        if(arity != 0) goto done;
    }

    ret = OK;

done:
    return ret;
}

int
array_to_json(char* buf, int* index, yajl_gen handle)
{
    int ret = ERROR;
    int i, arity;

    if(ei_decode_list_header(buf, index, &arity)) goto done;

    if(yajl_gen_array_open(handle) != yajl_gen_status_ok) goto done;
    for(i = 0; i < arity; i++)
    {
        if(value_to_json(buf, index, handle) != OK) goto done;
    }
    if(yajl_gen_array_close(handle) != yajl_gen_status_ok) goto done;

    //Pass over empty list tail.
    if(arity > 0)
    {
        if(ei_decode_list_header(buf, index, &arity)) goto done;
        if(arity != 0) goto done;
    }

    ret = OK;

done:
    return ret;
}

int
binary_to_json(char* buf, int* index, yajl_gen handle)
{
    int ret = ERROR;
    int type, size;

    if(ei_get_type(buf, index, &type, &size)) goto done;
    if(type != BINARY) goto done;
    *index += 5;

    unsigned char* string = (unsigned char*) (buf + *index);
    if(yajl_gen_string(handle, string, size) != yajl_gen_status_ok)
    {
        goto done;
    }
   
    *index += size;
    ret = OK;

done:
    return ret;
}

int
value_to_json(char* buf, int* index, yajl_gen handle)
{
    int ret = ERROR;
    int type, size;
    long lval;
    double dval;
    
    if(ei_get_type(buf, index, &type, &size)) goto done;
    
    if(type == VERSION)
    {
        if(ei_decode_version(buf, index, &type)) goto done;
        if(!value_to_json(buf, index, handle)) goto done;
    }
    else if(type == LONG || type == NEG_LONG)
    {
        if(ei_decode_long(buf, index, &lval)) goto done;
        if(yajl_gen_integer(handle, lval) != yajl_gen_status_ok) goto done;
    }
    else if(type == DOUBLE)
    {    
        if(ei_decode_double(buf, index, &dval)) goto done;
        if(yajl_gen_double(handle, dval) != yajl_gen_status_ok) goto done;
    }
    else if(type == ATOM)
    {
        if(atom_to_json(buf, index, handle) != OK) goto done;
    }
    else if(type == STRING)
    {
        if(string_to_json(buf, index, handle) != OK) goto done;
    }
    else if(type == TUPLE)
    {
        if(map_to_json(buf, index, handle) != OK) goto done;
    }
    else if(type == LIST)
    {
        if(array_to_json(buf, index, handle) != OK) goto done;
    }
    else if(type == EMPTY_LIST)
    {
        if(ei_decode_list_header(buf, index, &size)) goto done;
        if(size != 0) goto done;
        if(yajl_gen_array_open(handle) != yajl_gen_status_ok) goto done;
        if(yajl_gen_array_close(handle) != yajl_gen_status_ok) goto done;
    }
    else if(type == BINARY)
    {
        if(binary_to_json(buf, index, handle) != OK) goto done;
    }
    else
    {
        goto done;
    }
    
    ret = OK;

done:
    return ret;
}

int
encode_json(char* buf, int len, char** rbuf, int rlen)
{
    int index = 0;
    int ret = -1;
    ErlDrvBinary* rval = NULL;
    const unsigned char* json = NULL;
    unsigned int jsonlen = 0;

    yajl_gen_config config = {0, NULL};
    yajl_gen handle = yajl_gen_alloc(&config, NULL);

    if(value_to_json(buf, &index, handle) != OK) goto done;

    yajl_gen_status status = yajl_gen_get_buf(handle, &json, &jsonlen);
    if(status != yajl_gen_status_ok) goto done;

    rval = driver_alloc_binary(jsonlen);
    if(rval == NULL) goto done;

    memcpy(rval->orig_bytes, json, jsonlen);
    *rbuf = (char*) rval;
    //ret = jsonlen;
    ret = 0;

done:
    if(handle != NULL) yajl_gen_free(handle);
    return ret;
}
