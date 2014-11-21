#!/bin/bash

unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
     exename=erl
else
    exename='start //MAX werl.exe'
    #exename='erl.exe'
fi

# PATHS
paths="-pa"
paths=$paths" $PWD/ebin"
paths=$paths" $PWD/deps/*/ebin"

start_opts="$paths"

# ERLOCIPOOL start options
echo "------------------------------------------"
echo "Starting ERLOCIPOOL (Opts)"
echo "------------------------------------------"
echo "EBIN Path : $paths"
echo "------------------------------------------"

# Starting ERLOCIPOOL
$exename $start_opts -s erlocipool
