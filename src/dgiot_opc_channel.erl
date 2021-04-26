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
    State = #state{
        id = ChannelId,
        env = ChannelArgs
    },
    {ok, State}.

%% 初始化池子
handle_init(State) ->
    shuwa_mqtt:subscribe(<<"dgiot_opc_da_ack">>),
    shuwa_mqtt:subscribe(<<"dgiot_opc_da_scan">>),
    erlang:send_after(1000 * 10, self(), scan_opc),
    {ok, State}.

%% 通道消息处理,注意：进程池调用
handle_event(EventId, Event, _State) ->
    lager:info("channel ~p, ~p", [EventId, Event]),
    ok.

handle_message(scan_opc, #state{env = Env} = State) ->
    dgiot_opc:scan_opc(Env),
    {ok, State};

handle_message({deliver, <<"dgiot_opc_da_scan">>, Msg}, #state{id = ChannelId, env = Env} = State) ->
    shuwa_bridge:send_log(ChannelId, "from opc scan: ~p  ", [unicode:characters_to_list(shuwa_mqtt:get_payload(Msg))]),
    case shuwa_bridge:get_products(ChannelId) of
        {ok,_,[ProductId|_]} ->
            dgiot_opc:create_konva(ProductId, jsx:decode(shuwa_mqtt:get_payload(Msg), [{labels, binary}, return_maps]));
        _ -> pass
    end,
    {ok, State};

handle_message(Message, State) ->
    lager:info("channel ~p", [Message]),
    {ok, State}.

stop(ChannelType, ChannelId, _State) ->
    lager:info("channel stop ~p,~p", [ChannelType, ChannelId]),
    ok.
