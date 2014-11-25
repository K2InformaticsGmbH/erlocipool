-module(erlocipool_worker).
-behaviour(gen_server).

-include("erlocipool.hrl").

-record(session, {ssn, monitor, open_stmts = 0, closed_stmts = 0}).
-record(state, {name, type, owner, ociopts, logfun, tns, usr, passwd,
                sess_min = 0, sess_max = 0, last_error, shares = [],
                sessions = []}).

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
    OciOpts = proplists:get_value(ociopts, Opts),
    LogFun = proplists:get_value(logfun, Opts),
    Type = proplists:get_value(type, Opts, public),
    MinSession = proplists:get_value(sess_min, Opts, 10),
    MaxSession = proplists:get_value(sess_max, Opts, 20),
    erlang:send_after(0, self(), build_pool),
    {ok, #state{name = Name, type = Type, owner = Owner, ociopts = OciOpts,
                logfun = LogFun, tns = Tns, usr = User, passwd = Password,
                sess_min = MinSession, sess_max = MaxSession}}.

handle_call({sessions, Pid}, From, State) ->
    case handle_call({has_access, Pid}, From, State) of
        {reply, false, NewState} ->
            {reply, {error, private}, NewState};
        {reply, true, NewState} ->
            {reply, [{OciSession, O, C}
                     || #session{ssn = OciSession,
                                 open_stmts = O,
                                 closed_stmts = C} <- NewState#state.sessions],
             NewState}
    end;
handle_call({prep_sql, Pid, Sql}, From, State) ->
    case handle_call({has_access, Pid}, From, State) of
        {reply, false, NewState} ->
            {reply, {error, private}, NewState};
        {reply, true, NewState} ->
            case NewState#state.sessions of
                [] ->
                    {reply, {error, no_sessions}, NewState};
                Sessions ->
                    {Statement, NewSessions} = prep_sql(Sql, Sessions),
                    {reply, Statement,
                     NewState#state{sessions = NewSessions}}
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

prep_sql(Sql, Sessions) -> prep_sql(Sql, Sessions, {undefined, []}).
prep_sql(_Sql, [], {Statement, Sessions}) -> {Statement, Sessions};
prep_sql(Sql, [#session{
                  ssn = {oci_port, _, OciSessionHandle} = OciSession
                 } = Session | Sessions],
         {Statement, NewSessions}) ->
    case OciSession:prep_sql(Sql) of
        {oci_port, statement, _PortPid, OciSessionHandle, OciStatementHandle} ->
            ?DBG("prep_sql", "sql ~p, statement ~p", [Sql, OciStatementHandle]),
            prep_sql(Sql, [],
                     {{ok, {OciSessionHandle, OciStatementHandle}},
                      [Session#session{
                         open_stmts = Session#session.open_stmts + 1}
                       | Sessions] ++ NewSessions});
        Other ->
            ?DBG("prep_sql", "sql ~p, statement ~p~n", [Sql, Other]),
            prep_sql(Sql, [],
                     {Statement,
                      [Session | Sessions] ++ NewSessions})
    end.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(build_pool, State) ->
    case State#state.last_error of
        undefined ->
            case catch erloci:new(State#state.ociopts, State#state.logfun) of
                {'EXIT', {Error, _}} ->
                    erlang:send_after(?DELAY_RETRY_AFTER_ERROR, self(), build_pool),
                    {noreply, State#state{last_error = Error}};
                {oci_port, PortPid} = OciPort ->
                    case OciPort:get_session(State#state.tns, State#state.usr,
                                             State#state.passwd) of
                        {error, Error} ->
                            erlang:send_after(?DELAY_RETRY_AFTER_ERROR,
                                              self(), build_pool),
                            {noreply, State#state{last_error = Error}};
                        {oci_port, PortPid, _} = OciSession ->
                            if length(State#state.sessions) + 1
                               < State#state.sess_min ->
                                   erlang:send_after(0, self(), build_pool);
                                   true -> ok
                            end,
                            {noreply, State#state{
                                        sessions
                                        = [#session{
                                              ssn = OciSession,
                                              monitor = erlang:monitor(
                                                          process, PortPid)}
                                           | State#state.sessions]}}
                    end
            end;
        _Error ->            
            erlang:send_after(0, self(), build_pool),
            {noreply, State#state{last_error = undefined}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, State]) ->
    State.
