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

AC_DEFUN([COUCH_WINDOWS_HACK, [

# Windows Erlang build tools wrap Microsoft's linker and compiler just enough
# to be able to build Erlang/OTP successfully, but not enough for full
# compatibility with GNU AutoTools. The MS VC compiler and linker are
# hidden from autotools in Erlang's cc.sh and ld.sh wrappers. GNU autoconf
# identifies this dastardly mix as a unix variant, and libtool kindly
# passes incorrect flags and names through to the MS linker. The simplest fix
# is to modify libtool via sed to remove those options.
# As this is only done once at first configure, and subsequent config or source
# changes may trigger a silent reversion to the non-functioning original.
# Changes are;
# 1. replace LIB$name with $name in libname_spec (e.g. libicu -> icu) to ensure
#    correct windows versions of .lib and .dlls are found or generated.
# 2. remove incompatible \w-link\w from archive_cmds
# 3. remove GNU-style directives to be passed through to the linker
# 4. swap GNU-style shared library flags with MS -dll variant
# This obscene hackery is tracked under COUCHDB-440 and COUCHDB-1197.

if test x${IS_WINDOWS} = xTRUE; then
    mv libtool libtool.dist
    /bin/sed -E -e 's,^libname_spec="lib,libname_spec=",' \
        -e 's,( -link ), ,' \
        -e 's,-Xlinker --out-implib -Xlinker \\\$lib,,' \
        -e 's,(-shared -nostdlib), -dll ,' \
        < libtool.dist > libtool
    # probably would chmod +x if we weren't on windows...
fi

])