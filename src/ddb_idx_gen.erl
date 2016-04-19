-module(ddb_idx_gen).
-export([start/0, buckets/2, metrics/3, convert/3, insert_metrics/2]).
-include_lib("dproto/include/dproto.hrl").

start() ->
    application:ensure_all_started(?MODULE).


buckets(Host, Port) ->
    {ok, C} = ddb_tcp:connect(Host, Port),
    {ok, Buckets, C1} = ddb_tcp:list(C),
    ddb_tcp:close(C1),
    {ok, Buckets}.

metrics(Host, Port, Bucket) ->
    {ok, C} = ddb_tcp:connect(Host, Port),
    {ok, Keys, C1} = ddb_tcp:list(Bucket, C),
    ddb_tcp:close(C1),
    {ok, {Bucket, Keys}}.


convert({Bucket, Keys}, Glob, Translation) ->
    Keys1 = [{K, dproto:metric_to_list(K)} || K <- Keys],
    Keys2 = lists:filter(matcher(Glob), Keys1),
    {ok, [{Bucket, K, translate_metric(KL, Translation)} || {K, KL} <-Keys2]}.

insert_metrics(Collection, Keys) ->
    [dqe_idx:add(Collection, Metric, Bucket, Key, Tags) ||
        {Bucket, Key, {Metric, Tags}} <- Keys].

translate_metric(KeyL, {Base, Tags}) ->
    {dproto:metric_from_list(translate_base(KeyL, Base, [])),
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
