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

AC_DEFUN([COUCH_ICU, [

AC_LANG([C])

AC_ARG_WITH(
    [win32-icu-binaries],
    [
        AC_HELP_STRING(
            [--with-win32-icu-binaries=PATH],
            [set PATH to the Win32 native ICU binaries directory]
        )
    ],
    [
        ICU_CPPFLAGS="-I$withval/include"
        ICU_LIBS="-L$withval/lib -licuuc -licudt -licuin"
        ICU_BIN=$withval/bin
    ],
    [
        AC_CHECK_ICU([3.4.1])
        ICU_BIN=
    ]
)

AC_SUBST(ICU_CFLAGS)
AC_SUBST(ICU_CPPFLAGS)
AC_SUBST(ICU_LIBS)
AC_SUBST(ICU_BIN)

])