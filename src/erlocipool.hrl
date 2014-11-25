-ifndef(ERLOCIPOOL_HRL).
-define(ERLOCIPOOL_HRL, true).

-define(LOG_TAG, "OCIPOOL").

-include_lib("erloci/src/oci.hrl").

-define(value(__K, __L), proplists:get_value(__K, __L)).
-define(value(__K, __L, __D), proplists:get_value(__K, __L, __D)).

-define(POOL_GROWTH_DELAY, 1000).
-define(DELAY_RETRY_AFTER_ERROR, 10000).

-define(DBG(__Fun,__F,__A),
        io:format(user, "{~p,~p,"__Fun",~p} "__F"~n",
                  [?MODULE, ?LINE, self()|__A])
       ).
-define(DBG(__Fun,__F), ?DBG(__Fun,__F,[])).

-endif. % ERLOCIPOOL_HRL
