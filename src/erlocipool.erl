-module(erlocipool).

-behaviour(application).

%% Application callbacks
-export([start/0, stop/0]).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() ->
    application:start(erloci),
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE),
    application:stop(erloci).

start(_StartType, _StartArgs) ->
    erlocipool_sup:start_link().

stop(_State) ->
    ok.
