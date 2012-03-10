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

AC_DEFUN([COUCH_SNAPPY, [

m4_define([snappy_major], [1])
m4_define([snappy_minor], [0])
m4_define([snappy_patchlevel], [3])

AC_PROG_CXX
AC_LANG([C++])
AC_C_BIGENDIAN
AC_CHECK_HEADERS([stdint.h stddef.h sys/mman.h sys/resource.h])
AC_CHECK_FUNC([mmap])

AC_MSG_CHECKING([if the compiler supports __builtin_expect])

AC_TRY_COMPILE(,
    [return __builtin_expect(1, 1) ? 1 : 0],
    [
        snappy_have_builtin_expect=yes
        AC_MSG_RESULT([yes])
    ],
    [
        snappy_have_builtin_expect=no
        AC_MSG_RESULT([no])
    ]
)

if test x$snappy_have_builtin_expect = xyes ; then
    AC_DEFINE([HAVE_BUILTIN_EXPECT], [1], [Compiler supports __builtin_expect.])
fi

AC_MSG_CHECKING([if the compiler supports __builtin_ctzll])

AC_TRY_COMPILE(,
    [return (__builtin_ctzll(0x100000000LL) == 32) ? 1 : 0],
    [
        snappy_have_builtin_ctz=yes
        AC_MSG_RESULT([yes])
    ],
    [
        snappy_have_builtin_ctz=no
        AC_MSG_RESULT([no])
    ]
)

if test x$snappy_have_builtin_ctz = xyes ; then
    AC_DEFINE([HAVE_BUILTIN_CTZ], [1], [Compiler supports __builtin_ctz and friends.])
fi

if test "$ac_cv_header_stdint_h" = "yes"; then
    AC_SUBST([ac_cv_have_stdint_h], [1])
else
    AC_SUBST([ac_cv_have_stdint_h], [0])
fi

if test "$ac_cv_header_stddef_h" = "yes"; then
    AC_SUBST([ac_cv_have_stddef_h], [1])
else
    AC_SUBST([ac_cv_have_stddef_h], [0])
fi

SNAPPY_MAJOR="$snappy_major"
SNAPPY_MINOR="$snappy_minor"
SNAPPY_PATCHLEVEL="$snappy_patchlevel"

AC_SUBST([SNAPPY_MAJOR])
AC_SUBST([SNAPPY_MINOR])
AC_SUBST([SNAPPY_PATCHLEVEL])

])