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
                {error, no_sessions} -> timer:sleep(500);
                _ -> ok
            end,
            __Body,
            ?assertEqual(ok, Pool:del())
        end)()).

basic_test_() ->
    {timeout, 60, {
        setup, fun() -> erlocipool:start() end, fun(_) -> erlocipool:stop() end,
        {with, [fun obj_private_public/1,
                fun name_private_public/1
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

