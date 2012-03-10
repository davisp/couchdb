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

AC_DEFUN([COUCH_ERLANG], [

m4_define(
    [_ERL_VERSION_ERROR],
    [
The installed Erlang version is less than 5.6.5 (R12B05).]
)

m4_define(
    [_ERL_NO_HEADER_ERROR],
    [Could not find the 'erl_driver.h' header.

Are the Erlang headers installed? Use the '--with-erlang' option to specify the
path to the Erlang include directory.]
)

AC_LANG([C])

AC_ARG_VAR([ERL], [path to the 'erl' executable])
AC_PATH_PROG([ERL], [erl])

AC_ARG_VAR([ERLC], [path to the 'erlc' executable])
AC_PATH_PROG([ERLC], [erlc])

AC_ARG_VAR([ERLC_FLAGS], [general flags to prepend to ERLC_FLAGS])

if test x${ERL} = x; then
    AC_MSG_ERROR([Could not find the 'erl' executable. Is Erlang installed?])
fi

if test x${ERLC} = x; then
    AC_MSG_ERROR([Could not find the 'erlc' executable. Is Erlang installed?])
fi

ERL_VSN="`${ERL} -version 2>&1 | ${SED} 's/[[^0-9]]/ /g'`"
ERL_OTP_REL="`${ERL} -noshell -eval 'io:put_chars(erlang:system_info(otp_release)).' -s erlang halt`"
ERL_HAS_CRYPTO=`${ERL} -eval "case application:load(crypto) of ok -> ok; _ -> exit(no_crypto) end." -noshell -s init stop`

AC_MSG_CHECKING([for Erlang >= 5.6.5])

ERL_VSN_MAJOR=`echo $ERL_VSN | cut -d" " -f1`
ERL_VSN_MINOR=`echo $ERL_VSN | cut -d" " -f2`
ERL_VSN_PATCH=`echo $ERL_VSN | cut -d" " -f3`

if test $ERL_VSN_MAJOR -le 5; then
    if test $ERL_VSN_MINOR -le 6; then
        if test $ERL_VSN_PATCH -lt 5; then
            AC_MSG_ERROR([_ERL_VERSION_ERROR])
        fi
    fi
fi

AC_MSG_RESULT([ok])

AM_CONDITIONAL([USE_OTP_NIFS], [test x$ERL_OTP_REL \> xR13B03])
AM_CONDITIONAL([USE_EJSON_COMPARE_NIF], [test x$ERL_OTP_REL \> xR14B03])

AC_MSG_CHECKING([for the Erlang crypto library])
if test -n "$has_crypto"; then
    AC_MSG_ERROR([Could not find the Erlang crypto library. Has Erlang been compiled with OpenSSL support?])
fi
AC_MSG_RESULT([ok])

AC_ARG_WITH(
    [erlang],
    [AC_HELP_STRING([--with-erlang=PATH],
    [set PATH to the Erlang include directory])],
    [
        ERLANG_CFLAGS="-I$withval"
    ],
    [
        realerl=`readlink -f $ERL 2>/dev/null`
        AS_IF(
            [test $? -eq 0],
            [
                erlbase=`dirname $realerl`
                erlbase=`dirname $erlbase`
                ERL_CFLAGS="-I${erlbase}/usr/include"
            ],
            [
                # Failed to figure out where erl is installed..
                # try to add some default directories to search
                ERL_CFLAGS="-I${libdir}/erlang/usr/include"
                ERL_CFLAGS="$ERL_CFLAGS -I/usr/lib/erlang/usr/include"
                ERL_CFLAGS="$ERL_CFLAGS -I/usr/local/lib/erlang/usr/include"
                ERL_CFLAGS="$ERL_CFLAGS -I/opt/local/lib/erlang/usr/include"
            ]
        )
    ]
)

OLD_CFLAGS="$CPPFLAGS"
CFLAGS="$ERL_CFLAGS $CFLAGS"

AC_CHECK_HEADER(
    [erl_driver.h],
    [],
    [AC_MSG_ERROR([_ERL_NO_HEADER_ERROR])]
)

ERL_CFLAGS="$CFLAGS"
CFLAGS="$OLD_CFLAGS"

AC_SUBST(ERL_OTP_REL)

# Hack to optimize mochijson2

native_mochijson_enabled=no

AC_ARG_ENABLE(
    [native-mochijson],
    [
        AC_HELP_STRING(
            [--enable-native-mochijson],
            [compile mochijson to native code (EXPERIMENTAL)]
        )
    ],
    [native_mochijson_enabled=$enableval],
    []
)

AM_CONDITIONAL([USE_NATIVE_MOCHIJSON], [test x${native_mochijson_enabled} = xyes])

])