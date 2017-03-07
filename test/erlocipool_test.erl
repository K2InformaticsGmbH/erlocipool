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

-define(USINGPOOL(__Name, __Opts, __Body),
        (fun() ->
                 {ok, Pool} = erlocipool:new(__Name, ?TNS, ?USER, ?PASSWORD, __Opts),
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
          "s.type != 'BACKGROUND' and s.program like 'ocierl%'">>).
pool_test_() ->
    {timeout, 60, {
        setup,
        fun() ->
                erlocipool:start(),
                OciPort = erloci:new(
                            [{logging, true},
                             {env, [{"NLS_LANG",
                                     "GERMAN_SWITZERLAND.AL32UTF8"}]}]),
                OciSession = OciPort:get_session(?TNS, ?USER, ?PASSWORD),
                Stmt = OciSession:prep_sql(?SESSSQL),
                {cols, _} = Stmt:exec_stmt(),
                {{rows, SessBeforePool}, true} = Stmt:fetch_rows(10000),
                ok = Stmt:close(),
                {ok, Pool} = erlocipool:new(test_pub, ?TNS, ?USER, ?PASSWORD,
                                        [{type, public}, {sess_min, 2},
                                         {sess_max, 4}, {stmt_max, 1},
                                         {up_th, 50}, {down_th, 40},
                                         {ociOpts, [{ping_timeout, 1000}]}]),
                timer:sleep(500),
                {Pool, OciPort, OciSession, SessBeforePool}
        end,
        fun({Pool, OciPort, OciSession, SessBeforePool}) ->
                ?assertEqual(ok, erlocipool:del(test_pub)),
                {PoolSessns, SessAfterPool}
                = current_pool_session_ids(OciSession, SessBeforePool),
                ?debugFmt("Pool : ~p~nSessBeforePool : ~p~n"
                          "SessAfterPool : ~p~nPoolSess : ~p~n",
                          [Pool, lists:flatten(SessBeforePool),
                           lists:flatten(SessAfterPool), PoolSessns]),
                ?assertEqual(ok, application:stop(erlocipool)),
                Stmt1 = OciSession:prep_sql(?SESSSQL),
                timer:sleep(2000),
                ?assertMatch({cols, _}, Stmt1:exec_stmt()),
                ?assertEqual({{rows, SessBeforePool}, true},
                             Stmt1:fetch_rows(10000)),
                ?assertEqual(ok, Stmt1:close()),
                ?assertEqual(ok, OciSession:close()),
                ?assertEqual(ok, OciPort:close()),
                ?assertEqual(ok, application:stop(erloci))
        end,
        {with, [fun saturate_recover/1,
                fun bad_conn_recover/1
               ]}
    }}.

saturate_recover({Pool, _OciPort, _OciSession, _SessBefore}) ->
    % Loading Pool
    ?assertMatch([#{closed_stmts := 0, open_stmts := 0},
                  #{closed_stmts := 0, open_stmts := 0}], Pool:get_stats()),
    [S1] = stmts(Pool, 1),
    ?assertMatch([#{closed_stmts := 0, open_stmts := 0},
                  #{closed_stmts := 0, open_stmts := 1}], Pool:get_stats()),
    [S2] = stmts(Pool, 1),
    ?assertMatch([#{closed_stmts := 0, open_stmts := 0},
                  #{closed_stmts := 0, open_stmts := 1},
                  #{closed_stmts := 0, open_stmts := 1}], Pool:get_stats()),
    [S3, S4] = stmts(Pool, 2),
    ?assertMatch([#{closed_stmts := 0, open_stmts := 1},
                  #{closed_stmts := 0, open_stmts := 1},
                  #{closed_stmts := 0, open_stmts := 1},
                  #{closed_stmts := 0, open_stmts := 1}], Pool:get_stats()),

    % Pool full
    ?assertEqual({error, elimit}, Pool:prep_sql(<<"select * from dual">>)),

    % Depleting pool
    ?assertEqual(ok, S1:close()),

    ?assertMatch([#{closed_stmts := 1, open_stmts := 0},
                  #{closed_stmts := 0, open_stmts := 1},
                  #{closed_stmts := 0, open_stmts := 1},
                  #{closed_stmts := 0, open_stmts := 1}], Pool:get_stats()),
    ?assertEqual(ok, S2:close()),
    ?assertMatch([#{closed_stmts := 1, open_stmts := 0},
                  #{closed_stmts := 1, open_stmts := 0},
                  #{closed_stmts := 0, open_stmts := 1},
                  #{closed_stmts := 0, open_stmts := 1}], Pool:get_stats()),
    ?assertEqual(ok, S3:close()),
    ?assertMatch([#{closed_stmts := 1, open_stmts := 0},
                  #{closed_stmts := 1, open_stmts := 0},
                  #{closed_stmts := 1, open_stmts := 0},
                  #{closed_stmts := 0, open_stmts := 1}], Pool:get_stats()),
    ?assertEqual(ok, S4:close()),
    ?assertMatch([#{closed_stmts := 1, open_stmts := 0},
                  #{closed_stmts := 1, open_stmts := 0},
                  #{closed_stmts := 1, open_stmts := 0},
                  #{closed_stmts := 1, open_stmts := 0}], Pool:get_stats()),
    [S5,S6,S7,S8] = stmts(Pool, 4),
    ?assertMatch([#{closed_stmts := 0, open_stmts := 1},
                  #{closed_stmts := 0, open_stmts := 1},
                  #{closed_stmts := 1, open_stmts := 1},
                  #{closed_stmts := 1, open_stmts := 1}], Pool:get_stats()),
    ?assertEqual(ok, S5:close()),
    ?assertEqual(ok, S6:close()),
    ?assertEqual(ok, S7:close()),
    ?assertEqual(ok, S8:close()).

bad_conn_recover({Pool, _OciPort, OciSession, SessBefore}) ->
    % Loading Pool
    ?assertMatch([#{closed_stmts := 1, open_stmts := 0},
                  #{closed_stmts := 1, open_stmts := 0},
                  #{closed_stmts := 2, open_stmts := 0},
                  #{closed_stmts := 2, open_stmts := 0}], Pool:get_stats()),
    {ok, S} = Pool:prep_sql(<<"select * from dual">>),
    _ = Pool:get_stats(),
    ?assertMatch([#{closed_stmts := 2, open_stmts := 0},
                  #{closed_stmts := 1, open_stmts := 1}], Pool:get_stats()),
    {PoolSessns, _} = current_pool_session_ids(OciSession, SessBefore),
    ?assertEqual(ok, srv_kill_sessions(PoolSessns, OciSession)),
    timer:sleep(2000),
    ?assertMatch({error, _}, S:exec_stmt()),
    %% Pool replenished with new sessions
    ?assertMatch([#{closed_stmts := 0, open_stmts := 0},
                  #{closed_stmts := 0, open_stmts := 0},
                  #{closed_stmts := 0, open_stmts := 0},
                  #{closed_stmts := 0, open_stmts := 0}], Pool:get_stats()).

%------------------------
% Library functions
%

current_pool_session_ids(OciSession, SessBeforePool) ->
    Stmt = OciSession:prep_sql(?SESSSQL),
    ?assertMatch({cols, _}, Stmt:exec_stmt()),
    {{rows, SessAfterPool}, true} = Stmt:fetch_rows(10000),
    ?assertEqual(ok, Stmt:close()),
    {lists:flatten(SessAfterPool) -- lists:flatten(SessBeforePool),
     SessAfterPool}.

srv_kill_sessions([], _OciSession) -> ok;
srv_kill_sessions([Ps | PoolSessns], OciSession) ->
    Stmt = OciSession:prep_sql(
             list_to_binary(
               io_lib:format("alter system kill session '~s' immediate",
                             [Ps])
              )),
    case Stmt:exec_stmt() of
        {error,{30,<<"ORA-00030: User session ID does not exist.\n">>}} -> ok;
        {error,{31,<<"ORA-00031: session marked for kill\n">>}} -> ok;
        {executed, 0} ->
            ?debugFmt("~p closed", [Ps]),
            ?assertEqual(ok, Stmt:close())
    end,
    srv_kill_sessions(PoolSessns, OciSession).

stmts(Pool, N) -> stmts(Pool, N, []).
stmts(_Pool, 0, Acc) -> lists:reverse(Acc);
stmts(Pool, N, Acc) when N > 0 ->
    {ok, Stmt} = Pool:prep_sql(<<"select * from dual">>),
    ?assertEqual({cols, [{<<"DUMMY">>,'SQLT_CHR',2,0,0}]}, Stmt:exec_stmt()),
    ?assertEqual({{rows, [[<<"X">>]]}, true}, Stmt:fetch_rows(2)),
    stmts(Pool, N-1, [Stmt | Acc]).
