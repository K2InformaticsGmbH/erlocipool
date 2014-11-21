-ifndef(ERLOCIPOOL_HRL).
-define(ERLOCIPOOL_HRL, true).

-define(LOG_TAG, "OCIPOOL").

-include_lib("erloci/src/oci.hrl").

-define(value(__K, __L), proplists:get_value(__K, __L)).
-define(value(__K, __L, __D), proplists:get_value(__K, __L, __D)).

-endif. % ERLOCIPOOL_HRL
