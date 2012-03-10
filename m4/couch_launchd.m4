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

AC_DEFUN([COUCH_LAUNCHD, [

use_launchd=yes
launchd_enabled=false

AC_ARG_ENABLE(
    [launchd],
    [
        AC_HELP_STRING(
            [--disable-launchd],
            [don't install launchd configuration where applicable]
        )
    ],
    [use_launchd=$enableval],
    []
)

if test "$use_launchd" = "yes"; then
    AC_MSG_CHECKING(location of launchd directory)
    if test -d /Library/LaunchDaemons; then
        init_enabled=false
        launchd_enabled=true
        AC_SUBST([launchddir], ['${prefix}/Library/LaunchDaemons'])
        AC_MSG_RESULT(${launchddir})
    else
        AC_MSG_RESULT(not found)
    fi
fi

AM_CONDITIONAL([LAUNCHD], [test x${launchd_enabled} = xtrue])

])