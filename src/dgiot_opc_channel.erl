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
    #{<<"OPCSEVER">> := OpcServer, <<"productid">> := ProductId} = Env,
    case shuwa_parse:get_object(<<"Product">>, ProductId) of
        {ok, #{<<"thing">> := Thing,<<"name">> := ProductName}} ->
            #{<<"properties">> := Properties} = Thing,

            Identifier_item = [maps:get(<<"scan_instruct_item">>, H) || H <- Properties],
            Identifier_item1 = [binary:bin_to_list(H) || H <- Identifier_item],
            Identifier_Address = [maps:get(<<"scan_instruct_Address">>, H) || H <- Properties],
            Identifier_Address1 = [binary:bin_to_list(H) || H <- Identifier_Address],
            Identifier_Description = [maps:get(<<"scan_instruct_Description">>, H) || H <- Properties],
            Identifier_Description1 = [binary:bin_to_list(H) || H <- Identifier_Description],
            Identifier_RawDataType = [maps:get(<<"scan_instruct_RawDataType">>, H) || H <- Properties],
            Identifier_RawDataType1 = [binary:bin_to_list(H) || H <- Identifier_RawDataType],
            Identifier = Identifier_item1 ++ Identifier_Address1 ++ Identifier_Description1 ++ Identifier_RawDataType1,
            Instruct = [ X ++ "," || X <- Identifier],
            Instruct1 = lists:droplast(lists:concat(Instruct)),
            Instruct2 = erlang:list_to_binary(Instruct1),
            dgiot_opc:read_opc(ChannelId, OpcServer, ProductName, Instruct2),
            erlang:send_after(2000 * 10, self(), send_opc);
        _ ->
            handle_message(send_opc, #state{id = ChannelId, env = Env} = State)
    end,
    {ok, State};


%%{"status":0,"小闭式":{"INSPEC.小闭式台位计测.U_OPC":380,"INSPEC.小闭式台位计测.P_OPC":30}}
handle_message({deliver, _Topic, Msg}, #state{id = ChannelId, env = Env} = State) ->
    Payload = shuwa_mqtt:get_payload(Msg),
    #{<<"productid">> := ProductId, <<"deviceid">> := DeviceId, <<"devaddr">> := Devaddr} = Env,
    shuwa_bridge:send_log(ChannelId, "from opc scan: ~p  ", [Payload]),
    case jsx:is_json(Payload) of
        false ->
            pass;
        true ->
            case jsx:decode(Payload, [return_maps]) of
                #{<<"status">> := 0} = Map0 -> %% opc read的情况
                    [Map1 | _] = maps:values(maps:without([<<"status">>], Map0)),
                    case maps:find(<<"status">>,Map1) of
                        {ok,_} ->
                            [Map2 | _] = maps:values(maps:without([<<"status">>], Map1));
                        error ->
                            Map2 = Map1

                    end,
                    %%  ------------------------------------------------------------ read后更新物模型和组态
                    Data2 = maps:fold(fun(K, V, Acc) -> %% Data2 用于更新物模型
                        case binary:split(K, <<$.>>, [global, trim]) of
                            [_, _, Key1,Key2] ->
                                Key3 =erlang:list_to_binary(binary:bin_to_list(Key1) ++ binary:bin_to_list(Key2)),
                                Acc#{Key3 => V };
                            _ -> Acc
                        end
                                     end, #{}, Map2),
                    List_Data2 = maps:to_list(Data2),
                    Item_Data2 = case shuwa_parse:get_object(<<"Product">>, ProductId) of
                                     {ok, #{<<"thing">> := Thing}} ->
                                         #{<<"properties">> := Properties} = Thing,
                                         [maps:get(<<"identifier">>, H) || H <- Properties];
                                     _ -> []
                                     end,
                    Scan_instruct_item_Data2 = case shuwa_parse:get_object(<<"Product">>, ProductId) of
                                     {ok, #{<<"thing">> := Thing1}} ->
                                         #{<<"properties">> := Properties1} = Thing1,
                                         [maps:get(<<"scan_instruct_item">>, H) || H <- Properties1];
                                     _ -> []
                                 end,
                    Need_update_list = update_thing(List_Data2,Item_Data2,Scan_instruct_item_Data2),
                    Update_List = update_final_Properties(Need_update_list),
                    Num = erlang:length(Need_update_list),
                    Topo_para=lists:zip(Need_update_list,create_x_y(Num)),
                    Update_config = create_config(change_config(Topo_para)),
                    case  shuwa_parse:get_object(<<"Product">>, ProductId) of
                        {ok, #{<<"thing">> := Thing2}} ->
                            #{<<"need_update">> := Flag} = Thing2,
                            case Flag of
                                <<"true">> ->
                                    shuwa_parse:update_object(<<"Product">>, ProductId, #{<<"thing">> => #{<<"properties">> => Update_List,<<"need_update">> => <<"false">>}}),
                                    shuwa_parse:update_object(<<"Product">>, ProductId, #{<<"config">> =>  Update_config});
                                _ ->
                                    pass

                            end;
                        _ ->
                            pass
                    end,
                    %%  -------------------------------- 组态数据传递
                    Data = maps:fold(fun(K, V, Acc) ->
                        case binary:split(K, <<$.>>, [global, trim]) of
                            [_, _, Key] ->
                                Acc#{Key => V};
                            _ -> Acc
                        end
                                      end, #{}, Map2),

                    Base64 = get_optshape(ProductId, DeviceId, Data),
                    Url1 = <<"http://127.0.0.1:5080/iotapi/send_topo">>,
                    Data1 = #{<<"productid">> => ProductId, <<"devaddr">> => Devaddr, <<"base64">> => Base64},
                    push(Url1, Data1),
                    %%  -------------------------------- 设备上线状态修改
                    case shuwa_data:get({dev, status, DeviceId}) of
                        not_find ->
                            shuwa_data:insert({dev, status, DeviceId}, self()),
                            shuwa_parse:update_object(<<"Device">>, DeviceId, #{<<"status">> => <<"ONLINE">>});
                        _ -> pass

                    end,
                    shuwa_tdengine_adapter:save(ProductId, Devaddr, Data);
                _ ->
                    pass
            end,
            %%  ------------------------------------ opc scan 情况
            Map = jsx:decode(Payload, [return_maps]),
            List = maps:to_list(Map),
            D = dict:from_list(List),
            Predicate2=fun(X) ->
                X /= 95 end, % '_' -> 95 Unicode
            Predicate = fun(K,_V) ->
                lists:all(Predicate2,binary:bin_to_list(K)) end,
            D1 = dict:filter(Predicate, D),
            List1=dict:to_list(D1),
            %%  --------------------------------  scan后创建物模型
            case erlang:length(List1) of
                1 ->
                    {_,Dianwei} = lists:last(List1), %%Dianwei = [{"Name":"Acrel","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.Acrel","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"current","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.current","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"effect","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.effect","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"factor","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.factor","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"flow","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵 模拟.flow","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"head","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.head","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"opening","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.opening","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"power","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.power","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"pressure_in","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.pressure_in","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"pressure_out","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.pressure_out","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"speed","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模 拟.speed","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"switch","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.switch","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"temperature","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.temperature","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"torque","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.torque","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false},{"Name":"vol","HasChildren":false,"IsItem":true,"ItemId":"opc.水泵模拟.vol","ItemProperties":{"ErrorId":{"Failed":false,"Succeeded":true},"Properties":[]},"IsHint":false}]
                    Name_and_item = get_name_and_itemid(Dianwei),  %%Name_and_item = [{"Acrel","opc.水泵模拟.Acrel"},....]
                    Final_Properties = create_final_Properties(Name_and_item),
                    case shuwa_parse:get_object(<<"Product">>, ProductId) of
                        {ok, Result} ->
                            case maps:is_key(<<"thing">>,Result) of
                                false ->
                                    shuwa_parse:update_object(<<"Product">>, ProductId, #{<<"thing">> => #{<<"properties">> => Final_Properties,<<"need_update">> => <<"true">>}});
                                true ->
                                    pass
                            end;
                        Error2 -> lager:info("Error2 ~p ", [Error2])
                    end;
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
            Text = dgiot_topo:get_name(ProductId, K, V),
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


%%scan后创建物模型
create_Properties({Name,Identifier}) ->
    #{<<"accessMode">> => <<"r">>,
        <<"dataForm">> =>
        #{<<"address">> =>
        <<"00000000">>,
            <<"byteorder">> => <<"big">>,
            <<"collection">> => <<"%s">>,
            <<"control">> => <<"%q">>,<<"data">> => <<"null">>,
            <<"offset">> => 0,<<"protocol">> => <<"normal">>,
            <<"quantity">> => <<"null">>,<<"rate">> => 1,
            <<"strategy">> => <<"20">>},
        <<"dataType">> =>
        #{<<"specs">> =>
        #{<<"max">> => 100,<<"min">> => 0,
            <<"step">> => 0.01,<<"unit">> => <<>>},
            <<"type">> => <<"float">>},
        <<"identifier">> => Name,
        <<"name">> => Name,
        <<"scan_instruct_item">> => Identifier,
        <<"scan_instruct_Description">> => erlang:list_to_binary(binary:bin_to_list(Identifier) ++ "._Description"),
        <<"scan_instruct_RawDataType">> => erlang:list_to_binary(binary:bin_to_list(Identifier) ++ "._RawDataType"),
        <<"scan_instruct_Address">> => erlang:list_to_binary(binary:bin_to_list(Identifier) ++ "._Address"),
        <<"required">> => true}.



create_final_Properties(List) -> [ create_Properties(X) || X <- List].


add_to_list(Map) ->
    #{<<"Name">> := Name,<<"ItemId">> := ItemId } = Map,
    List =  binary:bin_to_list(Name),
    case lists:member(95,List) of
        true ->
            [];
        false ->
            [{Name,ItemId}]
    end.


get_name_and_itemid([H|T]) ->
    add_to_list(H) ++ get_name_and_itemid(T);
get_name_and_itemid([]) ->
    [].


%%%创建组态config
create_config(List) ->
        #{<<"konva">> =>
            #{<<"Stage">> =>
                #{<<"attrs">> =>
                    #{<<"draggable">> => true,<<"height">> => 469,
                    <<"id">> => <<"container">>,<<"width">> => 1868,
                    <<"x">> => 14,<<"y">> => 29},
                <<"children">> =>
                    [#{<<"attrs">> =>
                        #{<<"id">> => <<"Layer_sBE2t0">>},
                    <<"children">> =>
                        [#{<<"attrs">> =>
                            #{<<"height">> => 2000,
                            <<"id">> => <<"Group_9H6kPPA">>,
                            <<"width">> => 2000},
                        <<"children">> => List,              %%%组态按钮标签
                        <<"className">> => <<"Group">>}],
                    <<"className">> => <<"Layer">>}],
                <<"className">> => <<"Stage">>}}}.



%%创建组态按钮标签List->{text}





create_lable({[ID,Text,_,_,_],{X,Y}}) ->
    #{<<"attrs">> =>
    #{
        <<"draggable">> => true,
        <<"fill">> => <<"#000000">>,
        <<"fontFamily">> => <<"Calibri">>,
        <<"fontSize">> => 20,
        <<"id">> => ID,
        <<"text">> => Text, %% 太阳能板电压
        <<"type">> => <<"text">>,
        <<"x">> => X,
        <<"y">> => Y},
        <<"className">> => <<"Text">>}.



change_config(List) ->
    [ create_lable(X) || X <- List].

create_x_y(Num) when Num > 0 -> %% 根据属性个数生成合理的（x,y)坐标
    [{((Num-1) div 5)*300+100,50+150*((Num-1) rem 5)}|create_x_y(Num-1)];
create_x_y(0) -> [].
%% lists:zip({ID,Text},{X,Y}).


%% read后 更新物模型

update_thing(List,Item,Item2) ->
    ListA = create_all_list(List,Item),
    [create(X,Y)||X <- ListA,Y <- Item2,judge2(X,Y)].

judge(K,Item) ->
    [K1,_]= binary:split(K,<<$_>>, [global, trim]),
    case K1 == Item of
        true ->
            true;
        _ ->
            false
    end.

judge2(X,Y) ->
    [_,_,K]= binary:split(Y,<<$.>>, [global, trim]),
    [A,_,_] = proplists:get_keys(X),
    [Identifier,_] = binary:split(A,<<$_>>, [global, trim]),
    case K == Identifier of
        true ->
            true;
        _ ->
            false
    end.

create_list(List,H) ->
    [{K,V}||{K,V} <-List,judge(K,H)].

create_all_list(List,Item) ->
    [create_list(List,X)|| X <-Item].

create(X,Scan_Identifier) ->
    [A,B,C] = proplists:get_keys(X),
    [Identifier,A1] = binary:split(A,<<$_>>, [global, trim]),
    [_,B1]= binary:split(B,<<$_>>, [global, trim]),
    [_,C1]= binary:split(C,<<$_>>, [global, trim]),

    case <<"RawDataType">> of
        A1  ->
            DataType = proplists:get_value(A,X);
        B1 ->
            DataType = proplists:get_value(B,X);
        C1 ->
            DataType = proplists:get_value(C,X);
        _ ->
            DataType = <<"ERROR">>
    end,

    case <<"Description">> of
        A1->
            Description = proplists:get_value(A,X);
        B1 ->
            Description = proplists:get_value(B,X);
        C1 ->
            Description = proplists:get_value(C,X);
        _  ->
            Description= <<"ERROR">>
    end,

    case <<"Address">> of
        A1  ->
            Address= proplists:get_value(A,X);
        B1 ->
            Address = proplists:get_value(B,X);
        C1 ->
            Address = proplists:get_value(C,X);
        _ ->
            Address = <<"ERROR">>
    end,

    [Identifier,Description,DataType,Address,Scan_Identifier].

update_Properties([Identifier,Description,DataType,Address,Scan_Identifier]) ->
    #{<<"accessMode">> => <<"r">>,
        <<"dataForm">> =>
        #{<<"address">> =>Address,
            <<"byteorder">> => <<"big">>,
            <<"collection">> => <<"%s">>,
            <<"control">> => <<"%q">>,<<"data">> => <<"null">>,
            <<"offset">> => 0,<<"protocol">> => <<"normal">>,
            <<"quantity">> => <<"null">>,<<"rate">> => 1,
            <<"strategy">> => <<"20">>},
        <<"dataType">> =>
        #{<<"specs">> =>
        #{<<"max">> => 100,<<"min">> => 0,
            <<"step">> => 0.01,<<"unit">> => <<>>},
            <<"type">> => DataType},
        <<"identifier">> => Identifier,   %%SPEED
        <<"name">> => Description,
        <<"scan_instruct_item">> => Scan_Identifier, %% OPC.PUMP.SPEED
        <<"scan_instruct_Description">> => erlang:list_to_binary(binary:bin_to_list(Scan_Identifier) ++ "._Description"),
        <<"scan_instruct_RawDataType">> => erlang:list_to_binary(binary:bin_to_list(Scan_Identifier) ++ "._RawDataType"),
        <<"scan_instruct_Address">> => erlang:list_to_binary(binary:bin_to_list(Scan_Identifier) ++ "._Address"),
        <<"required">> => true}.

update_final_Properties(List) -> [ update_Properties(X) || X <- List].
