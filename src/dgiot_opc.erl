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
-module(dgiot_opc).
-author("johnliu").

-export([start_http/0, docroot/0]).

-export([
    scan_opc/1,
    read_opc/4,
    create_device/3,
    process_opc/2
]).

start_http() ->
    Port = application:get_env(dgiot_opc, port, 6080),
    DocRoot = docroot(),
    shuwa_http_server:start_http(?MODULE, Port, DocRoot).


docroot() ->
    {file, Here} = code:is_loaded(?MODULE),
    Dir = filename:dirname(filename:dirname(Here)),
    Root = shuwa_httpc:url_join([Dir, "/priv/"]),
    Root ++ "www".


%% 下发扫描OPC命令
%% topic: shuwa_opc_da
%% payload：
%%{
%%"cmdtype":"scan",
%%"opcserver":"Kepware.KEPServerEX.V6"
%%}
scan_opc(#{<<"OPCSEVER">> := OpcServer}) ->
    Payload = #{
        <<"cmdtype">> => <<"scan">>,
        <<"opcserver">> => OpcServer
    },
    shuwa_mqtt:publish(<<"opcserver">>, <<"dgiot_opc_da">>, jsx:encode(Payload)).

%%    {
%%        "Name":"Money",
%%        "HasChildren":false,
%%        "IsItem":true,
%%        "ItemId":"Write Only.Money",
%%        "ItemProperties":{
%%            "ErrorId":{
%%                "Failed":false,
%%                "Succeeded":true
%%            },
%%            "Properties":[
%%
%%            ]
%%        },
%%        "IsHint":false
%%    },
create_device(#{
    <<"app">> := App,
    <<"ACL">> := Acl,
    <<"devaddr">> := DtuAddr,
    <<"parentId">> := ParentId}, OPCPRODUCT, Items) ->
    pass.

get_thing(Product) ->
    case shuwa_parse:get_objectid(<<"Product">>, Product) of
        #{<<"objectId">> := ProductId} ->
            #{<<"objectId">> := DictId} =
                shuwa_parse:get_objectid(<<"Dict">>, #{<<"key">> => ProductId, <<"type">> => <<"Product">>}),
            case shuwa_parse:get_object(<<"Dict">>, DictId) of
                {ok, #{<<"data">> := #{<<"thing">> := Thing}}} ->
                    Thing;
                _ -> #{<<"properties">> =>[]}
            end;
        _ -> #{<<"properties">> =>[]}
    end.


read_opc(ChannelId, OpcServer, DevAddr, Instruct) ->
    [DevAddr | _] = maps:keys(Instruct),
    Values = maps:get(DevAddr, Instruct, #{}),
    Items =
        maps:fold(fun(K, _V, Acc) ->
            case Acc of
                <<"">> -> K;
                _ -> <<Acc/binary, ",", K/binary>>
            end
                  end, <<"">>, Values),
    Payload = #{
        <<"cmdtype">> => <<"read">>,
        <<"opcserver">> => OpcServer,
        <<"group">> => DevAddr,
        <<"items">> => Items
    },
    shuwa_bridge:send_log(ChannelId, "to_opc: ~p: ~p  ~ts ", [OpcServer, DevAddr, unicode:characters_to_list(Items)]),
    shuwa_mqtt:publish(<<"opcserver">>, <<"dgiot_opc_da">>, jsx:encode(Payload)).

process_opc(ChannelId, Payload) ->
    [DevAddr | _] = maps:keys(Payload),
    Items = maps:get(DevAddr, Payload, #{}),
    case shuwa_data:get({dgiot_opc, DevAddr}) of
        not_find ->
            pass;
        ProductId ->
            NewTopic = <<"thing/", ProductId/binary, "/", DevAddr/binary, "/post">>,
            shuwa_bridge:send_log(ChannelId, "to_task: ~ts", [unicode:characters_to_list(jsx:encode(Items))]),
            shuwa_mqtt:publish(DevAddr, NewTopic, jsx:encode(Items));
        _ -> pass
    end.
