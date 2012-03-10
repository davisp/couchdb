# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

AC_DEFUN([COUCH_JS, [

AC_LANG([C])

AC_ARG_WITH(
    [js-include],
    [AC_HELP_STRING([--with-js-include=PATH],
    [set PATH to the SpiderMonkey include directory])],
    [
        JS_INCLUDE="$withval"
        JS_CFLAGS="-I$JS_INCLUDE"
        JS_CFLAGS="$JS_CFLAGS -I$JS_INCLUDE/js"
        JS_CFLAGS="$JS_CFLAGS -I$JS_INCLUDE/mozjs"
    ],
    [
        PKG_CHECK_MODULES(
            [JS],
            [mozjs185],
            [JS_CFLAGS="$(${PKG_CONFIG} --cflags mozjs185)"]
            [
                PKG_CHECK_MODULES(
                    [JS],
                    [mozilla-js >= 1.7],
                    [JS_CFLAGS="$({PKG_CONFIG} --cflags mozilla-js)"],
                    [
                        JS_CFLAGS="-I/usr/include"
                        JS_CFLAGS="$JS_CFLAGS -I/usr/include/js"
                        JS_CFLAGS="$JS_CFLAGS -I/usr/include/mozjs"
                        JS_CFLAGS="$JS_CFLAGS -I/usr/local/include/js"
                        JS_CFLAGS="$JS_CFLAGS -I/opt/local/include/js"
                    ]
                )
            ]
        )
    ]
)

AC_ARG_WITH(
    [js-lib],
    [AC_HELP_STRING([--with-js-lib=PATH],
    [set PATH to the SpiderMonkey library directory])],
    [JS_LIB_DIR=$withval],
    [
        PKG_CHECK_MODULES(
            [JS],
            [mozjs185],
            [JS_LIB_DIR="$(${PKG_CONFIG} --variable=libdir mozjs185)"],
            [
                PKG_CHECK_MODULES(
                    [JS],
                    [mozilla-js >= 1.7],
                    [JS_LIB_DIR="$(${PKG_CONFIG} --variable=sdkdir mozilla-js)/lib"],
                    [JS_LIB_DIR="${libdir}"]
                )
            ]
        )
    ]
)

AC_ARG_WITH(
    [js-lib-name],
    [AC_HELP_STRING([--with-js-lib-name=NAME],
    [set Spidermonkey library NAME])],
    [
        JS_LIB_BASE="$withval"
        AC_CHECK_LIB([$JSLIB], JS_NewObject, [], [
            AC_MSG_ERROR([JS_BAD_LIB_NAME])])
    ],
    [
        AC_CHECK_LIB([mozjs185-1.0], [JS_NewObject], [JSLIB=mozjs185-1.0], [
            AC_CHECK_LIB([mozjs185], [JS_NewObject], [JSLIB=mozjs185], [
                AC_CHECK_LIB([mozjs], [JS_NewObject], [JSLIB=mozjs], [
                    AC_CHECK_LIB([js3250], [JS_NewObject], [JSLIB=js3250], [
                        AC_CHECK_LIB([js32], [JS_NewObject], [JSLIB=js32], [
                            AC_CHECK_LIB([js], [JS_NewObject], [JSLIB=js], [
                                AC_MSG_ERROR([JS_LIB_NOT_FOUND])
                            ])
                        ])
                    ])
                ])
            ])
        ])
    ]
)

use_js_trunk=no
AC_ARG_ENABLE(
    [js-trunk],
    [AC_HELP_STRING([--enable-js-trunk],
    [allow use of SpiderMonkey versions newer than js185-1.0.0])],
    [use_js_trunk=$enableval],
    []
)

AS_CASE(
    [$(uname -s)],
    [CYGWIN*],
    [
        JS_CFLAGS="-DXP_WIN $JS_CFLAGS"
    ],
    [*],
    [
        # XP_UNIX required for jsapi.h and has been
        # tested to work on Linux and Darwin.
        JS_CFLAGS="-DXP_UNIX $JS_CFLAGS"
    ]
)

AC_CHECK_HEADER(
    [jsapi.h],
    [],
    [
        AC_CHECK_HEADER(
            [js/jsapi.h],
            [JS_CFLAGS="$JS_CFLAGS -I$JS_INCLUDE/js"],
            [AC_MSG_ERROR([JS_HEADER_NOT_FOUND])]
        )
    ]
)

JS_LDFLAGS="-L$JS_LIB_DIR $LDFLAGS"

# Figure out what version of SpiderMonkey to use

AC_CHECK_LIB([$JS_LIB_BASE], [JS_NewCompartmentAndGlobalObject],
    # Prevent people from accidentally using SpiderMonkey's that are too new

    if test "$use_js_trunk" = "no"; then
        AC_CHECK_DECL([JSOPTION_ANONFUNFIX], [], [
            AC_MSG_ERROR([Your SpiderMonkey library is too new.

Versions of SpiderMonkey after the js185-1.0.0 release remove the optional
enforcement of preventing anonymous functions in a statement context. This
will most likely break your existing JavaScript code as well as render all
example code invalid.

If you wish to ignore this error pass --enable-js-trunk to ./configure.])],
        [[#include <jsapi.h>]])
    fi
    AC_DEFINE([SM185], [1], [Use SpiderMonkey 1.8.5])
)

AC_CHECK_LIB([$JS_LIB_BASE], [JS_ThrowStopIteration],
    AC_DEFINE([SM180], [1],
        [Use SpiderMonkey 1.8.0]))

AC_CHECK_LIB([$JS_LIB_BASE], [JS_GetStringCharsAndLength],
    AC_DEFINE([HAVE_JS_GET_STRING_CHARS_AND_LENGTH], [1],
        [Use newer JS_GetCharsAndLength function.]))

# Else, hope that 1.7.0 works

# Deal with JSScript -> JSObject -> JSScript switcheroo

AC_CHECK_TYPE([JSScript*],
    [AC_DEFINE([JSSCRIPT_TYPE], [JSScript*], [Use JSObject* for scripts])],
    [AC_DEFINE([JSSCRIPT_TYPE], [JSObject*], [Use JSScript* for scripts])],
    [[#include <jsapi.h>]]
)

AC_DEFINE([COUCHJS_NAME], ["couchjs"], ["CouchJS executable name."])