-module(ddb_idx_gen).
-export([start/0, buckets/2, metrics/3, convert/3, insert_metrics/2]).
-include_lib("dproto/include/dproto.hrl").

-type bucket() :: binary().
-type metric() :: [binary()].
-type glob_metric() :: [binary() | '*'].
-type substitution() :: {binary(), pos_integer()}.
-type translation() :: {[substitution()], [substitution()]}.
-type tag() :: {binary(), binary(), binary()}.

start() ->
    application:ensure_all_started(?MODULE).

-spec buckets(list(), pos_integer()) -> {ok, [bucket()]}.
buckets(Host, Port) ->
    {ok, C} = ddb_tcp:connect(Host, Port),
    {ok, Buckets, C1} = ddb_tcp:list(C),
    ddb_tcp:close(C1),
    {ok, Buckets}.

-spec metrics(list(), pos_integer(), bucket()) -> {ok, {bucket(), [metric()]}}.
metrics(Host, Port, Bucket) ->
    {ok, C} = ddb_tcp:connect(Host, Port),
    {ok, Keys, C1} = ddb_tcp:list(Bucket, C),
    ddb_tcp:close(C1),
    Keys1 = [dproto:metric_to_list(K) || K <- Keys],
    {ok, {Bucket, Keys1}}.

-spec convert({bucket(), [metric()]}, glob_metric(), translation()) ->
    {ok, [{bucket(), metric(), {metric(), [tag()]}}]}.

convert({Bucket, Keys}, Glob, Translation) ->
    Keys1 = [{dproto:metric_from_list(K), K} || K <- Keys],
    Keys2 = lists:filter(matcher(Glob), Keys1),
    {ok, [{Bucket, KL, translate_metric(KL, Translation)}
          || {_, KL} <- Keys2]}.

-spec insert_metrics(bucket(),
                     [{bucket(), metric(), {metric(), [tag()]}}]) -> term().
insert_metrics(Collection, Keys) ->
    [dqe_idx:add(Collection, Metric, Bucket, Key, Tags) ||
        {Bucket, Key, {Metric, Tags}} <- Keys].

translate_metric(KeyL, {Base, Tags}) ->
    {translate_base(KeyL, Base, []),
     translate_tags(KeyL, Tags, ddb_tags(KeyL))}.

ddb_tags(KeyL) ->
    ddb_tags(KeyL, 1, [{<<"ddb">>, <<"key_length">>,
                        integer_to_binary(length(KeyL))}]).
ddb_tags([], _, Tags) ->
    Tags;
ddb_tags([E | R], N, Tags) ->
    PosBin = integer_to_binary(N),
    T = {<<"ddb">>, <<"part_", PosBin/binary>>, E},
    ddb_tags(R, N + 1, [T | Tags]).

translate_base(_Metric, [], Acc) ->
    lists:reverse(Acc);
translate_base(Metric, [E | R], Acc) when is_binary(E) ->
    translate_base(Metric,  R, [E | Acc]);
translate_base(Metric, [N | R], Acc) when is_integer(N) ->
    translate_base(Metric,  R, [lists:nth(N, Metric) | Acc]).

translate_tags(_Metric, [], Acc) ->
    lists:reverse(Acc);
translate_tags(Metric, [{Name, E} | R], Acc) when is_binary(E) ->
    translate_tags(Metric,  R, [{<<>>, Name, E} | Acc]);
translate_tags(Metric, [{Name, N} | R], Acc) when is_integer(N) ->
    translate_tags(Metric,  R, [{<<>>, Name, lists:nth(N, Metric)} | Acc]).

matcher(Glob) ->
    fun ({Metric, _}) ->
            rmatch(Glob, Metric)
    end.


rmatch(['*' | Rm], <<_S:?METRIC_ELEMENT_SS/?SIZE_TYPE, _:_S/binary, Rb/binary>>) ->
    rmatch(Rm, Rb);
rmatch([_M | Rm], <<_S:?METRIC_ELEMENT_SS/?SIZE_TYPE, _M:_S/binary, Rb/binary>>) ->
    rmatch(Rm, Rb);
rmatch([], <<>>) ->
    true;
rmatch(_, _) ->
    false.
