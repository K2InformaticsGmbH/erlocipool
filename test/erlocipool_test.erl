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
            case Pool:prep_sql(<<"select * from dual">>) of
                {error, no_session} -> timer:sleep(500);
                _ -> ok
            end,
            __Body,
            ?assertEqual(ok, Pool:del())
        end)()).

basic_test_() ->
    {timeout, 60, {
        setup, fun() -> erlocipool:start() end, fun(_) -> erlocipool:stop() end,
        {with, [fun obj_private_public/1,
                fun name_private_public/1,
                fun fetch_data/1
               ]}
    }}.

obj_private_public(_) ->
    ?USINGPOOL(
       k2wks015_priv, [{type, private}],
       begin
           ?assertMatch({ok, _}, Pool:prep_sql(<<"select * from dual">>)),
           Self = self(),
           spawn(fun() ->
                         Self ! Pool:prep_sql(<<"select * from dual">>)
                 end),
           Result = receive R -> R end,
           ?assertEqual({error, private}, Result)
       end),
    ?USINGPOOL(
       k2wks015_pub, [{type, public}],
       begin
           ?assertMatch({ok, _}, Pool:prep_sql(<<"select * from dual">>)),
           Self = self(),
           spawn(fun() ->
                         Self ! Pool:prep_sql(<<"select * from dual">>)
                 end),
           Result = receive R -> R end,
           ?assertMatch({ok, _}, Result)
       end).

name_private_public(_) ->
    ?USINGPOOL(
       k2wks015_pub, [{type, public}],
       begin
           ?assertMatch({ok, _}, erlocipool:prep_sql(
                                   k2wks015_pub, <<"select * from dual">>)),
           Self = self(),
           spawn(fun() ->
                         Self ! erlocipool:prep_sql(
                                  k2wks015_pub, <<"select * from dual">>)
                 end),
           Result = receive R -> R end,
           ?assertMatch({ok, _}, Result)
       end),
    ?USINGPOOL(
       k2wks015_priv, [{type, private}],
       begin
           ?assertMatch({ok, _}, erlocipool:prep_sql(
                                   k2wks015_priv, <<"select * from dual">>)),
           Self = self(),
           spawn(fun() ->
                         Self ! erlocipool:prep_sql(
                                  k2wks015_priv, <<"select * from dual">>)
                 end),
           Result = receive R -> R end,
           ?assertMatch({error, private}, Result)
       end).

fetch_data(_) ->
    ?USINGPOOL(
       k2wks015_pub, [{type, public}],
       begin
           {ok, Stmt} = Pool:prep_sql(<<"select * from dual">>),
           ?assertMatch({cols, _}, Stmt:exec_stmt()),
           ?assertMatch({{rows, _}, true}, Stmt:fetch_rows(2)),
           Self = self(),
           spawn(fun() ->
                         Self ! Stmt:exec_stmt(),
                         Self ! Stmt:fetch_rows(2)
                 end),
           Cols = receive R -> R end,
           ?assertMatch({cols, _}, Cols),
           Rows = receive R1 -> R1 end,
           ?assertMatch({{rows, _}, true}, Rows)
       end),
    ?USINGPOOL(
       k2wks015_priv, [{type, private}],
       begin
           {ok, Stmt} = Pool:prep_sql(<<"select * from dual">>),
           ?assertMatch({cols, _}, Stmt:exec_stmt()),
           ?assertMatch({{rows, _}, true}, Stmt:fetch_rows(2)),
           Self = self(),
           spawn(fun() ->
                         Self ! Stmt:exec_stmt()
                 end),
           Cols = receive R -> R end,
           ?assertMatch({error, private}, Cols)
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
                                     "GERMAN_SWITZERLAND.AL32UTF8"}]}]),
                OciSession = OciPort:get_session(?TNS, ?USER, ?PASSWORD),
                Stmt = OciSession:prep_sql(?SESSSQL),
                {cols, _} = Stmt:exec_stmt(),
                {{rows, SessBeforePool}, true} = Stmt:fetch_rows(10000),
                ok = Stmt:close(),
                {ok, Pool} = erlocipool:new(test_pub, ?TNS, ?USER, ?PASSWORD,
                                        [{logfun, fun logfun/1},
                                         {type, public}]),
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
                [list_to_binary(
                   io_lib:format("alter system kill session '~s'", [Ps]))
                 || Ps <- PoolSess]
                 ]),
                ok = OciSession:close(),
                ok = OciPort:close(),
                erlocipool:stop()
        end,
        {with, []}
    }}.
