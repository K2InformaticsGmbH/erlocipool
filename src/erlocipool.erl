-module(erlocipool).

-behaviour(application).
-behaviour(supervisor).

%% Interface
-export([new/5]).

%% Application callbacks
-export([start/0, stop/0, start/2, stop/1]).

%% Supervisor callbacks
-export([init/1]).

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

new(Name, Tns, User, Password, Opts) ->
    supervisor:start_child(?SUPNAME, [Name, Tns, User, Password, Opts]).
