-module(erlocipool).

-behaviour(application).

%% Application callbacks
-export([start/0, stop/0]).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() -> application:start(?MODULE).
stop() -> application:stop(?MODULE).
start(_StartType, _StartArgs) ->
    erlocipool_sup:start_link().

stop(_State) ->
    ok.
