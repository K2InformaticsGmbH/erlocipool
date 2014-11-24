-module(erlocipool_worker).
-behaviour(gen_server).

-include("erlocipool.hrl").

-record(session, {ssn, monitor, leased, active_stmts,
                  total_stmtts}).
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
                          [Owner, Name, Tns, User, Password, Opts],
                          []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([Name, Owner, Tns, User, Password, Opts]) ->
    OciOpts = proplists:get_value(ociopts, Opts),
    LogFun = proplists:get_value(logfun, Opts),
    Type = proplists:get_value(type, Opts, private),
    MinSession = proplists:get_value(sess_min, Opts, 10),
    MaxSession = proplists:get_value(sess_max, Opts, 20),
    erlang:send_after(0, self(), build_pool),
    {ok, #state{name = Name, type = Type, owner = Owner, ociopts = OciOpts,
                logfun = LogFun, tns = Tns, usr = User, passwd = Password,
                sess_min = MinSession, sess_max = MaxSession}}.

handle_call({prep_sql, Pid}, From, State) ->
    case handle_call({has_access, Pid}, From, State) of
        {reply, false, NewState} ->
            {reply, {error, private}, NewState};
        {reply, true, NewState} ->
            [Session|_] = [S || S <- State#state.sessions,
                                S#session.leased == undefined],
            {reply, {?MODULE, self(), Session#session.ssn},
             NewState#state{sessions = [Session#session{leased = Pid}
                                        | State#state.sessions -- [Session]]}}
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
    {reply, if Pid == State#state.owner ->
                   true;
               true ->
                   lists:member(Pid, State#state.shares)
            end, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

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
