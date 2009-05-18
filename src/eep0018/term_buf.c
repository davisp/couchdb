/* Copyright (c) 2008-2009 Paul J. Davis <paul.joseph.davis@gmail.com>
 * Copyright (c) 2008-2009 Enrico Thierbach <eno@open-lab.org>
 *
 * This file is part of EEP0018, which is released under the MIT license.
 */

#include "eep0018.h"
#include "term_buf.h"

#define INIT_DBUF_SIZE 16
#define INIT_TBUF_SIZE 2048

#define CHECK_REQUIRE(BUF, N) if(!term_buf_require(BUF, N)) return ERROR;

dbl_buf*
dbl_buf_init()
{
    dbl_buf* ret = (dbl_buf*) malloc(sizeof(dbl_buf));
    if(ret == NULL) return NULL;

    ret->data = NULL;    
    ret->data = (double*) malloc(INIT_DBUF_SIZE * sizeof(double));
    if(ret->data == NULL)
    {
        free(ret);
        return NULL;
    }
    
    ret->length = INIT_DBUF_SIZE;
    ret->used = 0;
    return ret;
}

void
dbl_buf_destroy(dbl_buf* buf)
{
    if(buf == NULL) return;
    if(buf->data != NULL) free(buf->data);
    free(buf);
}

double*
dbl_buf_add(dbl_buf* buf, double val)
{
    double* next;
    if(buf->used >= buf->length)
    {
        buf->length *= 2;
        next = (double*) realloc(buf->data, buf->length * sizeof(double));
        if(next == NULL) return NULL;
    }
    
    buf->data[buf->used++] = val;
    return buf->data + (buf->used-1);
}

term_buf*
term_buf_init(void)
{
    int status = ERROR;
    term_buf* ret = (term_buf*) malloc(sizeof(term_buf));
    if(ret == NULL) goto done;
    
    ret->terms = NULL;
    ret->terms = (TermData*) malloc(INIT_TBUF_SIZE * sizeof(TermData));
    if(ret->terms == NULL) goto done;
    
    ret->doubles = NULL;
    ret->doubles = dbl_buf_init();
    if(ret->doubles == NULL) goto done;

    ret->length = INIT_TBUF_SIZE;
    ret->used = 0;

    status = OK;
done:
    if(status != OK)
    {
        term_buf_destroy(ret);
        ret = NULL;
    }
    return ret;
}

void
term_buf_destroy(term_buf* buf)
{
    if(buf == NULL) return;
    if(buf->doubles != NULL) dbl_buf_destroy(buf->doubles);
    if(buf->terms != NULL) free(buf->terms);
    free(buf);
}

int
term_buf_require(term_buf* buf, int want)
{
    int ret = ERROR;
    TermData* next;

    if(buf->length - buf->used > want) return OK;
    
    buf->length *= 2;
    next = (TermData*) realloc(buf->terms, buf->length * sizeof(TermData));
    if(next == NULL) goto done;

    buf->terms = next;
    ret = OK;

done:
    return ret;
}

int
term_buf_tuple(term_buf* buf, unsigned int elements)
{
    CHECK_REQUIRE(buf, 2);
    buf->terms[buf->used++] = ERL_DRV_TUPLE;
    buf->terms[buf->used++] = elements;
    return OK;
}

int
term_buf_list(term_buf* buf, unsigned int elements)
{
    CHECK_REQUIRE(buf, 3);
    buf->terms[buf->used++] = ERL_DRV_NIL;
    buf->terms[buf->used++] = ERL_DRV_LIST;
    buf->terms[buf->used++] = elements+1;
    return OK;
}

int
term_buf_binary(term_buf* buf, const void* data, unsigned int length)
{
    CHECK_REQUIRE(buf, 3);
    buf->terms[buf->used++] = ERL_DRV_BUF2BINARY;
    buf->terms[buf->used++] = (ErlDrvTermData) data;
    buf->terms[buf->used++] = length;
    return OK;
}

int
term_buf_true(term_buf* buf)
{
    CHECK_REQUIRE(buf, 2);
    buf->terms[buf->used++] = ERL_DRV_ATOM;
    buf->terms[buf->used++] = driver_mk_atom("true");
    return OK;
}

int
term_buf_false(term_buf* buf)
{
    CHECK_REQUIRE(buf, 2);
    buf->terms[buf->used++] = ERL_DRV_ATOM;
    buf->terms[buf->used++] = driver_mk_atom("false");
    return OK;
}

int
term_buf_null(term_buf* buf)
{
    CHECK_REQUIRE(buf, 2);
    buf->terms[buf->used++] = ERL_DRV_ATOM;
    buf->terms[buf->used++] = driver_mk_atom("null");
    return OK;
}

int
term_buf_int(term_buf* buf, int value)
{
    CHECK_REQUIRE(buf, 2);
    buf->terms[buf->used++] = ERL_DRV_INT;
    buf->terms[buf->used++] = (ErlDrvSInt) value;
    return OK;
}

int
term_buf_double(term_buf* buf, double value)
{
    CHECK_REQUIRE(buf, 2);

    double* pos = dbl_buf_add(buf->doubles, value);
    if(pos == NULL) return ERROR;
    
    buf->terms[buf->used++] = ERL_DRV_FLOAT;
    buf->terms[buf->used++] = (ErlDrvTermData) pos;

    return OK;
}
