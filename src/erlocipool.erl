-module(erlocipool).

-behaviour(application).
-behaviour(supervisor).

%% Create/destroy Pool APIs
-export([new/5, del/1]).

%% Using Pool APIs
-export([share/2, has_access/1, prep_sql/2, bind_vars/2, lob/4, exec_stmt/1,
         exec_stmt/2, fetch_rows/2, close/1, get_stats/1]).

%% Application callbacks
-export([start/0, stop/0, start/2, stop/1]).

%% Supervisor callbacks
-export([init/1]).

%% Logging callbacks
-export([loginfo/1]).

%% Helper macro for declaring children of supervisor
-define(SUPNAME, erlocipool_sup).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() ->
    application:start(erloci),
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE),
    application:stop(erloci).

start(_StartType, _StartArgs) -> start_link().
stop(_State) -> ok.

%% ===================================================================
%% Supervisor API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?SUPNAME}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, {{simple_one_for_one, 5, 10},
          [{erlocipool_worker, {erlocipool_worker, start_link, []},
            permanent, 5000, worker, [erlocipool_worker]}]}}.

%% ===================================================================
%% Create / Destroy Pool APIs
%% ===================================================================

new(Name, Tns, User, Password, Opts) ->
    case supervisor:start_child(
           ?SUPNAME, [Name, self(), Tns, User, Password, Opts]) of
        {ok, Child} -> {ok, {?MODULE, Child}};
        {error, Error} -> {error, Error}
    end.

del({?MODULE, Child}) -> del(Child);
del(undefined) -> {error, badpool};
del(PoolName) when is_atom(PoolName) -> del(whereis(PoolName));
del(Child) when is_pid(Child) -> supervisor:terminate_child(?SUPNAME, Child).

get_stats({?MODULE, PidOrName}) -> get_stats(PidOrName);
get_stats(PidOrName) -> gen_server:call(PidOrName, {sessions, self()}).

%% ===================================================================
%% Pool Access/Services APIs
%% ===================================================================

%
% Pool interface APIs
%
share(PidOrName, SharePid) ->
    gen_server:call(PidOrName, {share, self(), SharePid}).

has_access(PidOrName) -> gen_server:call(PidOrName, {has_access, self()}).

%
% Statement management APIs
%
% prep_sql and close internally checks the health of a connection if the
% operation returns error, it triggters a session ping in pool which might
% lead to a cleanup if session is dead
prep_sql(Sql, {?MODULE, PidOrName}) when is_binary(Sql) ->
    case gen_server:call(PidOrName, {prep_sql, self(), Sql}, infinity) of
        {ok, Ref} -> {ok, {?MODULE, PidOrName, Ref}};
        Other -> Other
    end;
prep_sql(PidOrName, Sql) when is_binary(Sql) ->
    prep_sql(Sql, {?MODULE, PidOrName}).

close({?MODULE, PidOrName, Ref}) ->
    gen_server:call(PidOrName, {close, self(), Ref}).

%
% Statement internal APIs
%
bind_vars(BindVars, {?MODULE, PidOrName, Ref}) ->
    gen_server:cast(PidOrName, {add_binds, Ref, BindVars}),
    stmt_op(PidOrName, Ref, bind_vars, [BindVars]).
lob(LobHandle, Offset, Length, {?MODULE, PidOrName, Ref}) ->
    stmt_op(PidOrName, Ref, lob, [LobHandle, Offset, Length]).

exec_stmt({?MODULE, PidOrName, Ref}) ->
    stmt_op(PidOrName, Ref, exec_stmt, []).
exec_stmt(BindVars, {?MODULE, PidOrName, Ref}) ->
    stmt_op(PidOrName, Ref, exec_stmt, [BindVars]).
%exec_stmt(BindVars, AutoCommit, {?MODULE, PidOrName, Stmt}) ->
%    stmt_op(PidOrName, Stmt, exec_stmt, [BindVars, AutoCommit]).

fetch_rows(Count, {?MODULE, PidOrName, Ref}) ->
    stmt_op(PidOrName, Ref, fetch_rows, [Count]).


% some errors from statement level APIs lob, exec_stmt, fetch_rows may result
% because of a closing/closed connection. So all the error paths triggters a
% session ping in pool which might lead to a cleanup if session is dead
stmt_op(PidOrName, Ref, Op, Args) ->
    case gen_server:call(PidOrName, {stmt, self(), Ref}) of
        {ok, ErlOciStmt} ->
            case apply(ErlOciStmt, Op, Args) of
                {error, {OraCode, _}} = Error when is_integer(OraCode) ->
                    gen_server:cast(PidOrName, {check, ErlOciStmt, OraCode}),
                    Error;
                {error, _} = Error ->
                    gen_server:cast(PidOrName, {check, ErlOciStmt}),
                    Error;
                Other -> Other
            end;
        Other -> Other
    end.

%% ===================================================================
%% Logging callback APIs
%% ===================================================================

loginfo({Level,ModStr,FunStr,Line,MsgStr}) ->
    case Level of
        info ->
            io:format("[~p] {~s,~s,~p} ~s~n",
                      [Level,ModStr,FunStr,Line,MsgStr]);
        _ -> ok
    end.
