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

# Ref : http://oracle-base.com/articles/misc/killing-oracle-sessions.php
# sqlplus sys/abcd123@80.67.144.206:5437 as sysdba
# sqlplus scott/regit@80.67.144.206:5437
#  SET LINESIZE 100
#  COLUMN spid FORMAT A10
#  COLUMN username FORMAT A10
#  COLUMN program FORMAT A45
#
#  SELECT s.inst_id,
#         s.sid,
#         s.serial#,
#         p.spid,
#         s.username,
#         s.program
#  FROM   gv$session s
#         JOIN gv$process p ON p.addr = s.paddr AND p.inst_id = s.inst_id
#  WHERE  s.type != 'BACKGROUND' and s.program = 'ocierl.exe';

#  SELECT '' || s.sid || ',' || s.serial#
#  FROM   gv$session s
#         JOIN gv$process p ON p.addr = s.paddr AND p.inst_id = s.inst_id
#  WHERE  s.type != 'BACKGROUND' and s.program = 'ocierl.exe';
# alter system kill session '136,4107';
