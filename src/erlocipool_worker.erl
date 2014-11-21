-module(erlocipool_worker).
-behaviour(gen_server).

-include("erlocipool.hrl").

-record(state, {port, session, tns, usr, passwd}).

% supervisor interface
-export([start_link/5]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, format_status/2]).

start_link(Name, Tns, User, Password, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Tns, User, Password, Opts],
                          []).

init([_Tns, _User, _Password, _Opts]) -> {ok, #state{}}.
handle_call(_Request, _From, State) -> {reply, ok, State}.
handle_cast(_Request, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
format_status(_Opt, [_PDict, State]) -> State.
