-module(erlocipool_worker).
-behaviour(gen_server).

-include("erlocipool.hrl").

-record(session, {ssn, monitor, openStmts = 0, closedStmts = 0}).
-record(state, {name, type, owner, ociOpts, logFun, tns, usr, passwd,
                sessMin = 0, sessMax = 0, stmtMax = 0, lastError, upTh = 0,
                shares = [], sessions = [] :: #session{}, downTh = 0}).

% supervisor interface
-export([start_link/6]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, format_status/2]).

%% ===================================================================
%% gen_server API functions
%% ===================================================================

start_link(Name, Owner, Tns, User, Password, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE,
                          [Name, Owner, Tns, User, Password, Opts],
                          []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([Name, Owner, Tns, User, Password, Opts]) ->
    try
        OciOpts = proplists:get_value(ociOpts, Opts, []),
        if is_list(OciOpts) -> ok;
           true -> exit({invalid, ociOpts}) end,

        LogFun = proplists:get_value(logfun, Opts),
        if is_function(LogFun, 1) orelse LogFun == undefined  -> ok;
           true -> exit({invalid, logfun}) end,

        Type = proplists:get_value(type, Opts, public),
        if Type == public orelse Type == private -> ok;
           true -> exit({invalid, type}) end,

        MinSessions = proplists:get_value(sess_min, Opts, 10),
        if is_integer(MinSessions) andalso MinSessions > 1 -> ok;
           true -> exit({invalid, sess_min}) end,

        MaxSessions = proplists:get_value(sess_max, Opts, 20),
        if is_integer(MaxSessions) andalso MaxSessions >= MinSessions -> ok;
           true -> exit({invalid, sess_max}) end,

        MaxStmtsPerSession = proplists:get_value(stmt_max, Opts, 20),
        if is_integer(MaxStmtsPerSession) andalso MaxStmtsPerSession > 0 -> ok;
           true -> exit({invalid, stmt_max}) end,
        
        UpThreshHold = proplists:get_value(up_th, Opts, 50),
        if is_number(UpThreshHold) andalso UpThreshHold > 1.0
           andalso UpThreshHold < 100.0 -> ok;
           true -> exit({invalid, up_th}) end,
        
        DownThreshHold = proplists:get_value(down_th, Opts, 40),
        if is_number(DownThreshHold) andalso DownThreshHold > 1.0 andalso
           DownThreshHold < 100.0 andalso DownThreshHold < UpThreshHold -> ok;
           true -> exit({invalid, down_th}) end,

        self() ! {build_pool, MinSessions},
        process_flag(trap_exit, true),
        {ok, #state{name = Name, type = Type, owner = Owner, ociOpts = OciOpts,
                    logFun = LogFun, tns = Tns, usr = User, passwd = Password,
                    sessMin = MinSessions, sessMax = MaxSessions,
                    stmtMax = MaxStmtsPerSession, upTh = UpThreshHold,
                    downTh = DownThreshHold}}
    catch
        _:Reason -> {stop, Reason}
    end.

handle_call({sessions, Pid}, From, State) ->
    case handle_call({has_access, Pid}, From, State) of
        {reply, false, NewState} ->
            {reply, {error, private}, NewState};
        {reply, true, NewState} ->
            {reply, [{OciSession, O, C}
                     || #session{ssn = OciSession,
                                 openStmts = O,
                                 closedStmts = C} <- NewState#state.sessions],
             NewState}
    end;
handle_call({stmt, Pid, {PortPid, OciSessnHandle, OciStmtHandle}}, From, State) ->
    case handle_call({has_access, Pid}, From, State) of
        {reply, false, NewState} ->
            {reply, {error, private}, NewState};
        {reply, true, NewState} ->
            case NewState#state.sessions of
                [] ->
                    {reply, {error, no_session}, NewState};
                Sessions ->
                    case [{oci_port, statement, PortPid, OciSessnHandle,
                           OciStmtHandle}
                          || #session{ssn = {oci_port, PP, OSessnH}}
                             <- Sessions, OSessnH == OciSessnHandle,
                             PP == PortPid] of
                        [Statement] ->
                            {reply, {ok, Statement}, NewState};
                        [] ->
                            {reply, {error, not_found}, NewState}
                    end
            end
    end;
handle_call({prep_sql, Pid, Sql}, From, State) ->
    case handle_call({has_access, Pid}, From, State) of
        {reply, false, State1} ->
            {reply, {error, private}, State1};
        {reply, true, State1} ->
            if length(State1#state.sessions) == 0 ->
                   {reply, {error, no_session}, State1};
               true ->
                   {Statement, State2}
                   = prep_sql(Sql, State1),
                   {reply, Statement, State2}
            end
    end;
handle_call({close, Pid, {PortPid, OciSessnHandle, OciStmtHandle}}, From, State) ->
    case handle_call({has_access, Pid}, From, State) of
        {reply, false, NewState} ->
            {reply, {error, private}, NewState};
        {reply, true, NewState} ->
            case NewState#state.sessions of
                [] ->
                    {reply, {error, no_session}, NewState};
                Sessions ->
                    case [{{oci_port, statement, PortPid, OciSessnHandle,
                           OciStmtHandle}, Session}
                          || #session{ssn = {oci_port, PP, OSessnH}}
                             = Session <- Sessions,
                             OSessnH == OciSessnHandle, PP == PortPid] of
                        [{Statement, Session}] ->
                            NewSessions =
                            [Session#session{
                               openStmts = Session#session.openStmts - 1,
                               closedStmts = Session#session.closedStmts + 1}
                             | Sessions -- [Session]],
                            {reply, Statement:close(),
                             NewState#state{sessions = sort_sessions(NewSessions)}};
                        [] ->
                            {reply, {error, not_found}, NewState}
                    end
            end
    end;
handle_call({share, Owner, SharePid}, _From, State) ->
    if State#state.type == private ->
           {reply, {error, private}, State};
       true ->
           if Owner == State#state.owner ->
                  {reply, ok, State#state{
                                shares = lists:usort([SharePid
                                                      | State#state.shares])}};
              true ->
                  {reply, {error, unauthorized}, State}
           end
    end;
handle_call({has_access, Pid}, _From, State) ->
    %?DBG("handle_call(has_access)", "Pid ~p, owner ~p, shares ~p",
    %     [Pid, State#state.owner, State#state.shares]),
    {reply,
     if State#state.type /= private -> true;
        true ->
            if Pid == State#state.owner -> true;
               true -> lists:member(Pid, State#state.shares)
            end
     end, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

-spec prep_sql(Sql :: binary(), State :: #state{}) ->
    {{ok, Statement :: tuple()} | {error, any()}, Sessions :: [#session{}]}.
prep_sql(Sql, #state{} = State) ->
    case pick_session(State) of
        {ok, #session{ssn = {oci_port, _, OciSessionHandle} = OciSsn} = Session,
         NewState} ->
            case OciSsn:prep_sql(Sql) of
                {oci_port, statement, PortPid, OciSessionHandle,
                 OciStatementHandle} ->
                    %?DBG("prep_sql", "sql ~p, statement ~p",
                    %[Sql, OciStatementHandle]),
                    {{ok, {PortPid, OciSessionHandle, OciStatementHandle}},
                     NewState#state{
                       sessions = sort_sessions(
                                    [Session#session{
                                       openStmts = Session#session.openStmts + 1}
                                     | NewState#state.sessions -- [Session]])
                      }};
                Other ->
                    ?DBG("prep_sql", "sql ~p, statement ~p~n", [Sql, Other]),
                    {{error, Other}, NewState}
            end;
        {error, Error} ->
            {{error, Error}, State}
    end.

-spec pick_session(State :: #state{}) ->
    {ok, Session :: #session{}, NewState :: #state{}}
    | {error, elimit}.
pick_session(#state{sessions = Sessions, sessMin = MinSess, sessMax = MaxSess,
                    stmtMax = MaxStmts, upTh = UpTh,
                    downTh = DownTh} = State) ->
    SessionCount = length(Sessions),
    SaturatedSessions = length([1 || #session{openStmts = Os} <- Sessions,
                                     Os >= MaxStmts]),
    SaturatedSessCent = SaturatedSessions / SessionCount * 100,
    
    if
        % UP
        SaturatedSessCent >= UpTh ->
            % Pool growth by 1 will be triggered if possible
            if SaturatedSessions >= MaxSess ->
                   % Pool is staurated
                   % (can't create any more connections or statements)
                   {error, elimit};
               SaturatedSessions < MaxSess andalso SessionCount == MaxSess ->
                   % Pool has reached growth limit. Reusing least used
                   % connection (from the front of the list) to create new
                   % statement
                   [Session | _] = Sessions,
                   {ok, Session, State};
               true ->
                   % Reuse the least used connection (from the front of the
                   % list) and also trigger pool growth by 1
                   self() ! {build_pool, 1},
                   [Session | _] = Sessions,
                   {ok, Session, State}
            end;
        % DOWN
        SaturatedSessCent =< DownTh andalso SessionCount > MinSess ->
            % Pool reduction check triggered
            % (may close old empty connections if exists)
            self() ! {check_reduce, SessionCount - MinSess},
            % New statement is assigned to second least loaded session.
            case Sessions of
                [#session{openStmts = Os1}, #session{openStmts = Os2} = S
                 | _] when Os1 =< Os2 andalso Os2 < MaxStmts ->
                    {ok, S, State};
                [#session{openStmts = Os1} = S, #session{openStmts = Os2}
                 | _] when Os1 < Os2 andalso Os2 >= MaxStmts ->
                    {ok, S, State};
                [#session{openStmts = Os1} = S] when Os1 < MaxStmts ->
                    {ok, S, State};
                _ -> {error, elimit}
            end;
        % HOLD
        true ->
            % Pool neither grow or reduce in this state. Reusing least used
            % connection (from the front of the list) to create new statement
            [Session | _] = Sessions,
            {ok, Session, State}
    end.

sort_sessions(Sessions) ->
    lists:sort(
      fun(#session{openStmts = OsA}, #session{openStmts = OsB}) ->
              if OsA =< OsB -> true; true -> false end
      end, Sessions).

handle_cast({kill, #session{
                      ssn = {oci_port, PortPid, _},
                      monitor = OciMon} = Session},
            State) ->
    try
        true = demonitor(OciMon, [flush]),
        OciPort = {oci_port, PortPid},
        ok = OciPort:close()
    catch
        _:Reason ->
            ?DBG("handle_cast(kill)", "error ~p~n~p",
                 [Reason, erlang:get_stacktrace()])
    end,
    self() ! {build_pool, State#state.sessMin - length(State#state.sessions)},
    {noreply, State#state{
                sessions = sort_sessions(State#state.sessions -- [Session])}};
handle_cast({check, {PortPid, OciSessnHandle, _OciStmtHandle}}, State) ->
    Self = self(),
    spawn(fun() ->
                  OciSession = {oci_port, PortPid, OciSessnHandle},
                  case catch OciSession:ping() of
                      ok -> ok;
                      _Error ->
                          case [S || #session{ssn={oci_port,PP,OSessnH}}=S
                                     <- State#state.sessions,
                                     OSessnH==OciSessnHandle, PP==PortPid] of
                              [S] -> gen_server:cast(Self, {kill, S});
                              _ -> ok
                          end
                  end
          end),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({check_reduce, ToClose},
            #state{sessions =
                   [#session{ssn = {oci_port, PortPid, _} = OciSession,
                             openStmts = 0} | Sessions]} = State)
  when ToClose > 0 ->
    OciSession:close(),
    OciPort = {oci_port, PortPid},
    OciPort:close(),
    self() ! {check_reduce, ToClose - 1},
    {noreply, State#state{sessions = sort_sessions(Sessions)}};
handle_info({check_reduce, _}, State) ->
    {noreply, State};

handle_info({build_pool, N}, State) ->
    case State#state.lastError of
        undefined ->
            if length(State#state.sessions) < State#state.sessMax
               andalso N > 0 ->
                   case catch erloci:new(State#state.ociOpts,
                                         State#state.logFun) of
                       {'EXIT', {Error, _}} ->
                           erlang:send_after(?DELAY_RETRY_AFTER_ERROR, self(),
                                             {build_pool, N}),
                           {noreply, State#state{lastError = Error}};
                       {oci_port, PortPid} = OciPort ->
                           case OciPort:get_session(State#state.tns,
                                                    State#state.usr,
                                                    State#state.passwd) of
                               {error, Error} ->
                                   OciPort:close(),
                                   erlang:send_after(?DELAY_RETRY_AFTER_ERROR,
                                                     self(), {build_pool, N}),
                                   {noreply, State#state{lastError = Error}};
                               {oci_port, PortPid, _} = OciSession ->
                                   self() ! {build_pool, N - 1},
                                   {noreply,
                                    State#state{sessions
                                                = [#session{
                                                      ssn = OciSession,
                                                      monitor = erlang:monitor(
                                                                  process,
                                                                  PortPid)}
                                                   | State#state.sessions]}}
                           end
                   end;
               true ->
                   {noreply, State}
            end;
        _Error ->
            self() ! {build_pool, N},
            {noreply, State#state{lastError = undefined}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{sessions = Sessions}) ->
    [begin
         OciPort = {oci_port, PortPid},
         catch OciPort:close()
     end || #session{ssn = {_, PortPid, _}} <- Sessions].

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, State]) ->
    State.
