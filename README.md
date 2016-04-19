ddb_idx_gen
=====

An OTP application

Build
-----

    $ rebar3 compile


Running
-------
this is still a bit of a manual process but a good starting point:

```erlang

%% Setup

application:set_env(dqe, max_read, 1209600).
application:set_env(dqe, get_chunk, 100000).
application:set_env(dqe_idx, lookup_module, dqe_idx_pg).
application:set_env(ddb_connection, backend, {"192.168.1.43", 5555}).
application:set_env(ddb_connection, pool_size, 5).
application:set_env(ddb_connection, pool_max, 5).

Host = "192.168.1.43", Port = 5555, Bucket = <<"fifo">>.
Glob = ['*', '*', '*', '*', '*'].
Translation = {[<<"action">>, 5], [{<<"service">>, 2}, {<<"entity">>, 3}, {<<"operation">>, 4}, {<<"host">>, 1}]}.

ddb_idx_gen:start().

pgapp:connect([{size, 10}, {database, "metric_metadata"}, {username, "ddb"}, {password, "ddb"}, {host, "192.168.1.43"}]).

{ok, Metrics} = ddb_idx_gen:metrics(Host, Port, Bucket).
{ok, Tagged} = ddb_idx_gen:convert(Metrics, Glob, Translation).
ddb_idx_gen:insert_metrics(<<"fifo">>, Tagged).

```
* Translations are a couple
    * first argument is the resulting metric, binaries remain the same and integers are replaced with the corresponding value in the key. For example 5 is replaced but the 5th element in the key.
    * second argument is the tags, each element in the list is a key value pair (tag name, tag value). The value is replaced the same way the metric name is.
* SQL, and backend connections are just in there as variables.
