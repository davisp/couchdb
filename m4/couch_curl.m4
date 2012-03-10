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

AC_DEFUN([COUCH_CURL, [

AC_LANG([C])

use_curl=yes

AC_ARG_WITH(
    [win32-curl],
    [
        AC_HELP_STRING(
            [--with-win32-curl=PATH],
            [set PATH to the Win32 native curl directory]
        )
    ],
    [
        # default build on windows is a static lib, and that's what we want too
        CURL_CFLAGS="-I$withval/include -DCURL_STATICLIB"
        CURL_LIBS="-L$withval/lib -llibcurl -lWs2_32 -lkernel32 -luser32 -ladvapi32 -lWldap32"
        # OpenSSL libraries may be pulled in via libcurl if it was built with SSL
        # these are libeay32 ssleay32 instead of crypto ssl on unix
    ],
    [
        AC_CHECK_CURL(
            [7.18.0],
            [AC_DEFINE([HAVE_CURL], [1], ["Provide HTTP support to couchjs"])],
            [
                AC_MSG_WARN(NO_CURL_SUPPORT)
                use_curl=no
                CURL_LIBS=
            ]
        )
    ]
)

case "$(uname -s)" in
  Linux)
    LIBS="$LIBS -lcrypt"
    CPPFLAGS="-D_XOPEN_SOURCE $CPPFLAGS"
    ;;
  FreeBSD)
    LIBS="$LIBS -lcrypt"
    ;;
  OpenBSD)
    LIBS="$LIBS -lcrypto"
  ;;
esac

AM_CONDITIONAL([USE_CURL], [test x${use_curl} = xyes])
AC_SUBST(CURL_CFLAGS)
AC_SUBST(CURL_LIBS)

])