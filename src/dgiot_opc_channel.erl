%%--------------------------------------------------------------------
%% Copyright (c) 2020 DGIOT Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(dgiot_opc_channel).
-behavior(shuwa_channelx).
-define(TYPE, <<"DGIOTOPC">>).
-author("johnliu").
-record(state, {id, env = #{}}).

%% API
-export([start/2]).
-export([init/3, handle_event/3, handle_message/2, handle_init/1, stop/3]).


%% 注册通道类型
-channel(?TYPE).
-channel_type(#{
    type => 1,
    title => #{
        zh => <<"OPC采集通道"/utf8>>
    },
    description => #{
        zh => <<"OPC采集通道"/utf8>>
    }
}).
%% 注册通道参数
-params(#{
    <<"OPCSEVER">> => #{
        order => 1,
        type => string,
        required => true,
        default => <<"Kepware.KEPServerEX.V6"/utf8>>,
        title => #{
            zh => <<"OPC服务器"/utf8>>
        },
        description => #{
            zh => <<"OPC服务器"/utf8>>
        }
    },
    <<"OPCGROUP">> => #{
        order => 2,
        type => string,
        required => true,
        default => <<"group"/utf8>>,
        title => #{
            zh => <<"OPC分组"/utf8>>
        },
        description => #{
            zh => <<"OPC分组"/utf8>>
        }
    }
}).

start(ChannelId, ChannelArgs) ->
    shuwa_channelx:add(?TYPE, ChannelId, ?MODULE, ChannelArgs#{
        <<"Size">> => 1
    }).

%% 通道初始化
init(?TYPE, ChannelId, ChannelArgs) ->
    {ProductId, DeviceId, Devaddr} = get_product(ChannelId),
    State = #state{
        id = ChannelId,
        env = ChannelArgs#{<<"productid">> => ProductId,
            <<"deviceid">> => DeviceId,
            <<"devaddr">> => Devaddr}
    },
    {ok, State}.

%% 初始化池子
handle_init(State) ->
    shuwa_mqtt:subscribe(<<"dgiot_opc_da_ack">>),
    shuwa_mqtt:subscribe(<<"dgiot_opc_da_scan">>),
    shuwa_parse:subscribe(<<"Device">>, post),
%%    erlang:send_after(1000 * 10, self(), scan_opc),
    erlang:send_after(1000 * 10, self(), send_opc),
    {ok, State}.

%% 通道消息处理,注意：进程池调用
handle_event(EventId, Event, _State) ->
    lager:info("channel ~p, ~p", [EventId, Event]),
    ok.

handle_message({sync_parse, Args}, State) ->
    lager:info("sync_parse ~p", [Args]),
    {ok, State};

handle_message(scan_opc, #state{env = Env} = State) ->
    dgiot_opc:scan_opc(Env),
    {ok, State};

%% {"cmdtype":"read",  "opcserver": "ControlEase.OPC.2",   "group":"小闭式",  "items": "INSPEC.小闭式台位计测.U_OPC,INSPEC.小闭式台位计测.P_OPC,
%%    INSPEC.小闭式台位计测.I_OPC,INSPEC.小闭式台位计测.DJZS_OPC,INSPEC.小闭式台位计测.SWD_OPC,
%%    INSPEC.小闭式台位计测.DCLL_OPC,INSPEC.小闭式台位计测.JKYL_OPC,INSPEC.小闭式台位计测.CKYL_OPC","noitemid":"000"}
handle_message(send_opc, #state{id = ChannelId, env = Env} = State) ->
    #{<<"OPCSEVER">> := OpcServer} = Env,
    Instruct = <<"INSPEC.小闭式台位计测.U_OPC,INSPEC.小闭式台位计测.P_OPC,INSPEC.小闭式台位计测.I_OPC,INSPEC.小闭式台位计测.DJZS_OPC,INSPEC.小闭式台位计测.SWD_OPC,INSPEC.小闭式台位计测.DCLL_OPC,INSPEC.小闭式台位计测.JKYL_OPC,INSPEC.小闭式台位计测.CKYL_OPC"/utf8>>,
    dgiot_opc:read_opc(ChannelId, OpcServer, <<"XBS001">>, Instruct),
    erlang:send_after(10 * 1000, self(), send_opc),
    {ok, State};


%%{"status":0,"小闭式":{"INSPEC.小闭式台位计测.U_OPC":380,"INSPEC.小闭式台位计测.P_OPC":30}}
handle_message({deliver, _Topic, Msg}, #state{id = ChannelId, env = Env} = State) ->
    Payload = shuwa_mqtt:get_payload(Msg),
    #{<<"productid">> := ProductId, <<"deviceid">> := DeviceId, <<"devaddr">> := Devaddr} = Env,
    shuwa_bridge:send_log(ChannelId, "from opc scan: ~p  ", [unicode:characters_to_list(Payload)]),
    case jsx:is_json(Payload) of
        false ->
            pass;
        true ->
            case jsx:decode(Payload, [return_maps]) of
                #{<<"status">> := 0} = Map ->
                    [Map1 | _] = maps:values(maps:without([<<"status">>], Map)),
                    Data = maps:fold(fun(K, V, Acc) ->
                        case binary:split(K, <<$.>>, [global, trim]) of
                            [_, _, Key] ->
                                Acc#{Key => V};
                            _ -> Acc
                        end
                                     end, #{}, Map1),
                    Base64 = get_optshape(ProductId, DeviceId, Data),
                    Url1 = <<"http://132.232.12.21/iotapi/send_topo">>,
                    Data1 = #{<<"productid">> => ProductId, <<"devaddr">> => Devaddr, <<"base64">> => Base64},
                    push(Url1, Data1),
                    case shuwa_data:get({dev, status, DeviceId}) of
                        not_find ->
                            shuwa_data:insert({dev, status, DeviceId}, self()),
                            shuwa_parse:update_object(<<"Device">>, DeviceId, #{<<"status">> => <<"ONLINE">>});
                        _ -> pass

                    end,
                    shuwa_tdengine_adapter:save(ProductId, Devaddr, Data);
                _ ->
                    pass
            end
    end,
    {ok, State};

handle_message(Message, State) ->
    lager:info("channel ~p", [Message]),
    {ok, State}.

stop(ChannelType, ChannelId, _State) ->
    lager:info("channel stop ~p,~p", [ChannelType, ChannelId]),
    ok.

get_product(ChannelId) ->
    case shuwa_bridge:get_products(ChannelId) of
        {ok, _, [ProductId | _]} ->
            Filter = #{<<"where">> => #{<<"product">> => ProductId}, <<"limit">> => 1},
            case shuwa_parse:query_object(<<"Device">>, Filter) of
                {ok, #{<<"results">> := Results}} when length(Results) == 1 ->
                    [#{<<"objectId">> := DeviceId, <<"devaddr">> := Devaddr} | _] = Results,
                    {ProductId, DeviceId, Devaddr};
                _ ->
                    {<<>>, <<>>, <<>>}
            end;
        _ ->
            {<<>>, <<>>, <<>>}
    end.


get_optshape(ProductId, DeviceId, Payload) ->
    Shape =
        maps:fold(fun(K, V, Acc) ->
            Text = dgiot_topo:get_name(ProductId, K, shuwa_utils:to_binary(V)),
            Type =
                case shuwa_data:get({shapetype, shuwa_parse:get_shapeid(ProductId, K)}) of
                    not_find ->
                        <<"text">>;
                    Type1 ->
                        Type1
                end,
            Acc ++ [#{<<"id">> => shuwa_parse:get_shapeid(DeviceId, K), <<"text">> => Text, <<"type">> => Type}]
                  end, [], Payload),
    base64:encode(jsx:encode(#{<<"konva">> => Shape})).


push(Url, Data) ->
    Url1 = shuwa_utils:to_list(Url),
    Data1 = shuwa_utils:to_list(jsx:encode(Data)),
    httpc:request(post, {Url1, [], "application/json", Data1}, [], []).

