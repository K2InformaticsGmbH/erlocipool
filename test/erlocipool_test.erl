-module(erlocipool_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("erlocipool/src/erlocipool.hrl").

-define(TNS,
        <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)"
          "(HOST=80.67.144.206)(PORT=5437)))(CONNECT_DATA=(SERVICE_NAME=XE))"
          ")">>
       ).
-define(USER, <<"scott">>).
-define(PASSWORD, <<"regit">>).

logfun(_Log) -> ok.
-define(USINGPOOL(__Name, __Opts, __Body),
        (fun() ->
                 {ok, Pool} = erlocipool:new(__Name, ?TNS, ?USER, ?PASSWORD,
                                             [{logfun, fun logfun/1}|__Opts]),
                 (fun Sessions() ->
                    case Pool:get_stats() of
                        [] ->
                            timer:sleep(1),
                            Sessions();
                        _ -> ok
                    end
                 end)(),
                 __Body,
                 ?assertEqual(ok, Pool:del())
        end)()).

basic_test_() ->
    {timeout, 600, {
        setup, fun() -> erlocipool:start() end, fun(_) -> erlocipool:stop() end,
        {with, [
                fun private_public/1,
                fun fetch_data/1
               ]}
    }}.

private_public(_) ->
    ?USINGPOOL(
       k2wks015_priv, [{type, private}],
       begin
           %?debugHere,
           {ok, Stmt} = Pool:prep_sql(<<"select * from dual">>),
           ?assertEqual(ok, Stmt:close()),
           Self = self(),
           spawn(fun() ->
                         Self ! Pool:prep_sql(<<"select * from dual">>)
                 end),
           ?assertEqual({error, private}, receive R -> R end),
           {ok, Stmt1} = erlocipool:prep_sql(k2wks015_priv, <<"select * from dual">>),
           ?assertEqual(ok, Stmt1:close()),
           spawn(fun() ->
                         Self ! erlocipool:prep_sql(
                                  k2wks015_priv, <<"select * from dual">>)
                 end),
           %?debugHere,
           ?assertMatch({error, private}, receive R1 -> R1 end)
       end),
    ?USINGPOOL(
       k2wks015_pub, [{type, public}],
       begin
           %?debugHere,
           {ok, Stmt} = Pool:prep_sql(<<"select * from dual">>),
           ?assertEqual(ok, Stmt:close()),
           Self = self(),
           spawn(fun() ->
                         Self ! Pool:prep_sql(<<"select * from dual">>)
                 end),
           {ok, Stmt1} = receive R -> R end,
           ?assertMatch(ok, Stmt1:close()),
           {ok, Stmt2} = erlocipool:prep_sql(k2wks015_pub, <<"select * from dual">>),
           ?assertEqual(ok, Stmt2:close()),
           spawn(fun() ->
                         Self ! erlocipool:prep_sql(
                                  k2wks015_pub, <<"select * from dual">>)
                 end),
           {ok, Stmt3} = receive R1 -> R1 end,
           %?debugHere,
           ?assertEqual(ok, Stmt3:close())
       end).

fetch_data(_) ->
    ?USINGPOOL(
       k2wks015_pub, [{type, public}],
       begin
           %?debugHere,
           {ok, Stmt} = Pool:prep_sql(<<"select * from dual">>),
           Self = self(),
           spawn(fun() ->
                         ?assertMatch({cols, _}, Stmt:exec_stmt()),
                         ?assertMatch({{rows, _}, true}, Stmt:fetch_rows(2)),
                         ?assertEqual(ok, Stmt:close()),
                         Self ! done
                 end),
           %?debugHere,
           ?assertEqual(done, receive R -> R after 10000 -> timeout end)
       end),
    ?USINGPOOL(
       k2wks015_priv, [{type, private}],
       begin
           %?debugHere,
           {ok, Stmt} = Pool:prep_sql(<<"select * from dual">>),
           Self = self(),
           spawn(fun() ->
                         Self ! Stmt:exec_stmt()
                 end),
           ?assertMatch({error, private}, receive R -> R end),
           %?debugHere,
           ?assertEqual(ok, Stmt:close())
       end).

-define(SESSSQL,
        <<"select '' || s.sid || ',' || s.serial# from gv$session s join "
          "gv$process p on p.addr = s.paddr and p.inst_id = s.inst_id where "
          "s.type != 'BACKGROUND' and s.program = 'ocierl.exe'">>).
pool_test_() ->
    {timeout, 60, {
        setup,
        fun() ->
                erlocipool:start(),
                OciPort = erloci:new(
                            [{logging, true},
                             {env, [{"NLS_LANG",
                                     "GERMAN_SWITZERLAND.AL32UTF8"}]}],
                            fun logfun/1),
                OciSession = OciPort:get_session(?TNS, ?USER, ?PASSWORD),
                Stmt = OciSession:prep_sql(?SESSSQL),
                {cols, _} = Stmt:exec_stmt(),
                {{rows, SessBeforePool}, true} = Stmt:fetch_rows(10000),
                ok = Stmt:close(),
                {ok, Pool} = erlocipool:new(test_pub, ?TNS, ?USER, ?PASSWORD,
                                        [{logfun, fun logfun/1},
                                         {type, public}, {sess_min, 1},
                                         {sess_max, 2}, {stmt_max, 2},
                                         {up_th, 50}, {down_th, 40}]),
                timer:sleep(1000),
                Stmt1 = OciSession:prep_sql(?SESSSQL),
                {cols, _} = Stmt1:exec_stmt(),
                {{rows, SessAfterPool}, true} = Stmt1:fetch_rows(10000),
                ok = Stmt1:close(),
                {Pool, OciPort, OciSession, SessBeforePool, SessAfterPool}
        end,
        fun({Pool, OciPort, OciSession, SessBeforePool, SessAfterPool}) ->
                PoolSess = lists:flatten(SessAfterPool)
                -- lists:flatten(SessBeforePool),
                io:format(user, "~p Pool session ~p~n", [Pool,
                [begin
                     Stmt = OciSession:prep_sql(
                              list_to_binary(
                                io_lib:format("alter system kill session '~s'",
                                              [Ps]))
                             ),
                     {executed, 0} = Stmt:exec_stmt(),
                     ok = Stmt:close(),
                     Ps
                 end || Ps <- PoolSess]
                 ]),
                Stmt = OciSession:prep_sql(?SESSSQL),
                {cols, _} = Stmt:exec_stmt(),
                ?assertEqual({{rows, SessBeforePool}, true}, Stmt:fetch_rows(10000)),
                ok = Stmt:close(),
                ok = OciSession:close(),
                ok = OciPort:close(),
                erlocipool:stop()
        end,
        {with, [fun saturate_recover/1]}
    }}.

saturate_recover({Pool, _OciPort, _OciSession, _SessBefore, _SessAfter}) ->
    Stats = Pool:get_stats(),
    ?assertEqual(1, length(Stats)),
    ?assertMatch([{_,0,0}], Stats),
    Sql = <<"select * from dual">>,
    {ok, Stmt} = Pool:prep_sql(Sql),
    ?assertEqual({cols, [{<<"DUMMY">>,'SQLT_CHR',1,0,0}]}, Stmt:exec_stmt()),
    ?assertEqual({{rows, [[<<"X">>]]}, true}, Stmt:fetch_rows(2)),
    Stats1 = Pool:get_stats(),
    ?assertEqual(2, length(Stats1)),
    ?assertMatch([{_,0,0}, {_,1,0}], Stats1),
    ?assertEqual(ok, Stmt:close()),
    Stats2 = Pool:get_stats(),
    ?assertEqual(2, length(Stats2)),
    ?assertMatch([{_,0,0}, {_,0,1}], Stats2).
