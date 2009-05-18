/* Copyright (c) 2008-2009 Paul J. Davis <paul.joseph.davis@gmail.com>
 * Copyright (c) 2008-2009 Enrico Thierbach <eno@open-lab.org>
 *
 * This file is part of EEP0018, which is released under the MIT license.
 */

#include "eep0018.h"

static ErlDrvData
eep0018_start(ErlDrvPort port, char *buff)
{
    if(port == NULL) return ERL_DRV_ERROR_GENERAL;
    set_port_control_flags(port, PORT_CONTROL_FLAG_BINARY);
    return (ErlDrvData) port;
}

static int
eep0018_control(
        ErlDrvData drv_data,
        unsigned int command,
        char* buf,
        int len,
        char** rbuf,
        int rlen
)
{
    switch(command)
    {
        case 0:
            return encode_json(buf, len, rbuf, rlen);
        case 1:
            return decode_json((ErlDrvPort) drv_data, buf, len, rbuf, rlen);
        default:
            return -1;
    }
}

static ErlDrvEntry
eep0018_driver_entry =
{
    NULL,               /* Init */
    eep0018_start,
    NULL,               /* Stop */
    NULL,               /* Output */
    NULL,               /* Input Ready */
    NULL,               /* Output Ready */
    "eep0018_drv",      /* Driver Name */
    NULL,               /* Finish */
    NULL,               /* Handle */
    eep0018_control,    /* Control */
    NULL,               /* Timeout */
    NULL,               /* Outputv */
    NULL,               /* Ready Async */
    NULL,               /* Flush */
    NULL,               /* Call */
    NULL,               /* Event */
    ERL_DRV_EXTENDED_MARKER,
    ERL_DRV_EXTENDED_MAJOR_VERSION,
    ERL_DRV_EXTENDED_MINOR_VERSION,
    ERL_DRV_FLAG_USE_PORT_LOCKING,
    NULL,               /* Reserved */
    NULL,               /* Process Exit */
};

DRIVER_INIT(eep0018_drv)    /* must match name in driver_entry */
{
    return &eep0018_driver_entry;
}
