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
-record(state, {id, step , env = #{}}).

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
        env = ChannelArgs#{
            <<"productid">> => ProductId,
            <<"deviceid">> => DeviceId,
            <<"devaddr">> => Devaddr}
    },
    {ok, State}.

%% 初始化池子
handle_init(State) ->
    shuwa_mqtt:subscribe(<<"dgiot_opc_da_ack">>),
    shuwa_mqtt:subscribe(<<"dgiot_opc_da_scan">>),
    shuwa_parse:subscribe(<<"Device">>, post),
    erlang:send_after(1000 * 5, self(), scan_opc),
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
    {ok, State#state{step = scan}};

%% {"cmdtype":"read",  "opcserver": "ControlEase.OPC.2",   "group":"小闭式",  "items": "INSPEC.小闭式台位计测.U_OPC,INSPEC.小闭式台位计测.P_OPC,
%%    INSPEC.小闭式台位计测.I_OPC,INSPEC.小闭式台位计测.DJZS_OPC,INSPEC.小闭式台位计测.SWD_OPC,
%%    INSPEC.小闭式台位计测.DCLL_OPC,INSPEC.小闭式台位计测.JKYL_OPC,INSPEC.小闭式台位计测.CKYL_OPC","noitemid":"000"}
handle_message(read_opc, #state{id = ChannelId, step = read_cycle ,env = #{<<"OPCSEVER">> := OpcServer, <<"productid">> := ProductId,<<"devaddr">> := DevAddr}} = State) ->
    case shuwa_shadow:lookup_prod(ProductId) of
        {ok, #{<<"thing">> := #{<<"properties">> := Properties}}} ->
            Item = [maps:get(<<"dataForm">>, H) || H <- Properties],
            Item2 =[maps:get(<<"address">>, H) || H <- Item],
            Identifier_item = [binary:bin_to_list(H) || H <- Item2],
            Instruct = [X ++ "," || X <- Identifier_item],
            Instruct1 = lists:droplast(lists:concat(Instruct)),
            Instruct2 = erlang:list_to_binary(Instruct1),
            dgiot_opc:read_opc(ChannelId, OpcServer, DevAddr,Instruct2);
        _ ->
            pass
    end,
    {ok, State#state{step = read}};

%%{"status":0,"小闭式":{"INSPEC.小闭式台位计测.U_OPC":380,"INSPEC.小闭式台位计测.P_OPC":30}}
handle_message({deliver, _Topic, Msg}, #state{id = ChannelId, step = scan, env = Env} = State) ->
    Payload = shuwa_mqtt:get_payload(Msg),
    #{<<"OPCSEVER">> := OpcServer,<<"OPCGROUP">> := Group } = Env,
    shuwa_bridge:send_log(ChannelId, "from opc scan: ~p  ", [Payload]),
    case jsx:is_json(Payload) of
        false ->
            {ok, State};
        true ->
            dgiot_opc:scan_opc_ack(Payload,OpcServer, Group),
            {ok, State#state{step = pre_read}}
    end;

handle_message({deliver, _Topic, Msg}, #state{ step = pre_read, env = Env} = State) ->
    Payload = shuwa_mqtt:get_payload(Msg),
    #{<<"productid">> := ProductId} = Env,
    case jsx:is_json(Payload) of
        false ->
            pass;
        true ->
            case jsx:decode(Payload, [return_maps]) of
                #{<<"status">> := 0} = Map0 ->
                    [Map1 | _] = maps:values(maps:without([<<"status">>], Map0)),
                    case maps:find(<<"status">>,Map1) of
                        {ok,_} ->
                            [Map2 | _] = maps:values(maps:without([<<"status">>], Map1));
                        error ->
                            Map2 = Map1

                    end,
                    Data = maps:fold(fun(K, V, Acc) ->
                        case binary:split(K, <<$.>>, [global, trim]) of
                            [_, _, Key1,Key2] ->
                                Key3 =erlang:list_to_binary(binary:bin_to_list(Key1) ++ binary:bin_to_list(Key2)),
                                Acc#{Key3 => V,Key1 =>Key1 };
                            [_,_,_] ->
                                Acc#{K => K};
                            _ -> Acc
                        end
                                      end, #{}, Map2),
                    List_Data = maps:to_list(Data),
                    Need_update_list = dgiot_opc:create_changelist(List_Data),
                    Final_Properties = dgiot_opc:create_final_Properties(Need_update_list),
                    Topo_para=lists:zip(Need_update_list,dgiot_opc:create_x_y(erlang:length(Need_update_list))),
                    New_config = dgiot_opc:create_config(dgiot_opc:change_config(Topo_para)),
                    shuwa_product:load(ProductId),
                    case shuwa_shadow:lookup_prod(ProductId) of
                        {ok, #{<<"thing">> := #{<<"properties">> := Properties}}} ->
                            case erlang:length(Properties)  of
                                0 ->
                                    shuwa_parse:update_object(<<"Product">>, ProductId, #{<<"config">> =>  New_config}),
                                    shuwa_parse:update_object(<<"Product">>, ProductId, #{<<"thing">> => #{<<"properties">> => Final_Properties}});
                                _ ->
                                    pass
                            end
                    end,
                    {ok, State#state{step = read}}
            end
    end;



handle_message({deliver, _Topic, Msg}, #state{id = ChannelId, step = read, env = Env} = State) ->
    Payload = shuwa_mqtt:get_payload(Msg),
    #{<<"productid">> := ProductId, <<"deviceid">> := DeviceId, <<"devaddr">> := Devaddr} = Env,
    shuwa_bridge:send_log(ChannelId, "from opc read: ~p  ", [jsx:decode(Payload, [return_maps])]),
    case jsx:is_json(Payload) of
        false ->
            pass;
        true ->
            dgiot_opc:read_opc_ack(Payload, ProductId, DeviceId, Devaddr),
            erlang:send_after(1000 * 10, self(), read_opc)

    end,
    {ok, State#state{step = read_cycle}};

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

