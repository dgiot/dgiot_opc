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
    <<"OPCBRAND">> => #{
        order => 1,
        type => string,
        required => true,
        default => <<"KEPServerEX"/utf8>>,
        title => #{
            zh => <<"OPC厂商"/utf8>>
        },
        description => #{
            zh => <<"OPC厂商"/utf8>>
        }
    },
    <<"OPCSEVER">> => #{
        order => 2,
        type => string,
        required => true,
        default => <<"Kepware.KEPServerEX.V6"/utf8>>,
        title => #{
            zh => <<"OPC服务器"/utf8>>
        },
        description => #{
            zh => <<"OPC服务器"/utf8>>
        }
    }
}).

start(ChannelId, ChannelArgs) ->
    shuwa_channelx:add(?TYPE, ChannelId, ?MODULE, ChannelArgs#{
        <<"Size">> => 1
    }).

%% 通道初始化
init(?TYPE, ChannelId, #{<<"product">> := Products} = ChannelArgs) ->
    NewEnv = get_newenv(ChannelArgs),
    [{ProductId, App} | _] = get_app(Products),
    State = #state{
        id = ChannelId,
        env = NewEnv#{
            <<"app">> => App,
            <<"product">> => ProductId
        }
    },
    {ok, State}.

%% 初始化池子
handle_init(#state{env = #{<<"product">> := ProductId} = Env} = State) ->
    shuwa_mqtt:subscribe(<<"dgiot_opc_da_ack">>),
    shuwa_mqtt:subscribe(<<"dgiot_opc_da_scan">>),
    {ok,[DevId, DevAddr,Acl]} = get_device(ProductId),
    erlang:send_after(1000 * 10, self(), scan_opc),
    {ok, State#state{env = Env#{ <<"ACL">> => Acl,  <<"parentId">> => DevId, <<"devaddr">> => DevAddr} }}.

%% 通道消息处理,注意：进程池调用
handle_event(EventId, Event, _State) ->
    lager:info("channel ~p, ~p", [EventId, Event]),
    ok.

handle_message(scan_opc, #state{env = Env} = State) ->
    shuwa_plc:scan_opc(Env),
    {ok, State};

handle_message({deliver, <<"dgiot_opc_da_scan">>, Msg}, #state{id = ChannelId, env = Env} = State) ->
    shuwa_bridge:send_log(ChannelId,"from opc scan: ~p  ", [unicode:characters_to_list(shuwa_mqtt:get_payload(Msg))]),
    maps:fold(fun(OPCPRODUCT, Items, _Acc) ->
        shuwa_plc:create_device(Env, OPCPRODUCT, Items)
              end, [], jsx:decode(shuwa_mqtt:get_payload(Msg), [{labels, binary}, return_maps])),
    {ok, State};

handle_message({deliver, <<"dgiot_opc_da_ack">>, Msg}, #state{id = ChannelId, env = _Env} = State) ->
    shuwa_bridge:send_log(ChannelId,"from opc read: ~ts  ", [unicode:characters_to_list(shuwa_mqtt:get_payload(Msg))]),
    case jsx:decode(shuwa_mqtt:get_payload(Msg), [{labels, binary}, return_maps]) of
        #{<<"status">> := 0} = Payload ->
            shuwa_plc:process_opc(ChannelId, maps:without([<<"status">>], Payload));
        _ -> pass
    end,
    {ok, State};

handle_message({deliver, _Topic, Msg},
        #state{id = ChannelId, env = #{<<"OPCSEVER">> := OpcServer} = _Env} = State) ->
    case binary:split(shuwa_mqtt:get_topic(Msg), <<$/>>, [global, trim]) of
        [<<"thing">>, ProductId, DevAddr, _] ->
            shuwa_bridge:send_log(ChannelId, "from_task: ~ts:  ~ts ", [_Topic, unicode:characters_to_list(shuwa_mqtt:get_payload(Msg))]),
            #{<<"interval">> := _Interval, <<"instruct">> := Instruct, <<"appdata">> := _AppData} =
                jsx:decode(shuwa_mqtt:get_payload(Msg), [{labels, binary}, return_maps]),
            shuwa_data:insert({dgiot_opc,DevAddr},ProductId),
            dgiot_opc:read_opc(ChannelId, OpcServer, DevAddr, Instruct);
        %%接收task汇聚过来的整个dtu物模型采集的数据
        [_App, ProductId, DevAddr] ->
            shuwa_bridge:send_log(ChannelId, "from_app: ~ts:  ~ts ", [_Topic, unicode:characters_to_list(shuwa_mqtt:get_payload(Msg))]),
            Data = jsx:decode(shuwa_mqtt:get_payload(Msg), [{labels, binary}, return_maps]),
            dgiot_opc:save_opc(ProductId, DevAddr, Data, _Env);
        _ -> pass
    end,
    {ok, State};

handle_message(Message, State) ->
    lager:info("channel ~p", [Message]),
    {ok, State}.

stop(ChannelType, ChannelId, _State) ->
    lager:info("channel stop ~p,~p", [ChannelType, ChannelId]),
    ok.

get_app(Products) ->
    lists:map(fun({_ProdcutId, #{<<"ACL">> := Acl}}) ->
        Predicate = fun(E) ->
            case E of
                <<"role:", _/binary>> -> true;
                _ -> false
            end
                    end,
        [<<"role:", _App/binary>> | _] = lists:filter(Predicate, maps:keys(Acl)),
        {_ProdcutId, _App}
              end, Products).

get_device(ProdcutId) ->
    lager:info("ProdcutId ~p",[ProdcutId]),
    case shuwa_parse:query_object(<<"Device">>, #{<<"where">> => #{<<"product">> => ProdcutId}, <<"limit">> => 1}) of
        {ok,#{<<"results">> := [#{<<"objectId">> := DevId, <<"devaddr">> := DevAddr, <<"ACL">> := Acl} | _]}} ->
            {ok,[DevId, DevAddr, Acl]};
        _ ->
            {error,<<"not find">>}
    end.

get_newenv(Args) ->
    maps:without([
        <<"behaviour">>,
        <<"MaxOverFlow">>,
        <<"Size">>,
        <<"applicationtText">>,
        <<"product">>], Args).