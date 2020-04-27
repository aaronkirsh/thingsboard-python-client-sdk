from mqtt_as import MQTTClient, config
MQTTClient.DEBUG = True  # Optional
import uasyncio as asyncio
import ubinascii
import time
from json import loads, dumps
import ssl
from machine import unique_id
import logging
log = logging.getLogger(__name__)

RPC_RESPONSE_TOPIC = 'v1/devices/me/rpc/response/'
RPC_REQUEST_TOPIC = 'v1/devices/me/rpc/request/'
ATTRIBUTES_TOPIC = 'v1/devices/me/attributes'
ATTRIBUTES_TOPIC_REQUEST = 'v1/devices/me/attributes/request/'
ATTRIBUTES_TOPIC_RESPONSE = 'v1/devices/me/attributes/response/'
TELEMETRY_TOPIC = 'v1/devices/me/telemetry'

class TBTimeoutException(Exception):
    pass

class TBQoSException(Exception):
    pass

class TBPublishInfo():
    TB_ERR_AGAIN = -1
    TB_ERR_SUCCESS = 0
    TB_ERR_NOMEM = 1
    TB_ERR_PROTOCOL = 2
    TB_ERR_INVAL = 3
    TB_ERR_NO_CONN = 4
    TB_ERR_CONN_REFUSED = 5
    TB_ERR_NOT_FOUND = 6
    TB_ERR_CONN_LOST = 7
    TB_ERR_TLS = 8
    TB_ERR_PAYLOAD_SIZE = 9
    TB_ERR_NOT_SUPPORTED = 10
    TB_ERR_AUTH = 11
    TB_ERR_ACL_DENIED = 12
    TB_ERR_UNKNOWN = 13
    TB_ERR_ERRNO = 14
    TB_ERR_QUEUE_SIZE = 15

    def __init__(self, messageInfo):
        self.messageInfo = messageInfo

    def rc(self):
        return self.messageInfo.rc

    def mid(self):
        return self.messageInfo.mid

    def get(self):
        self.messageInfo.wait_for_publish()
        return self.messageInfo.rc


class TBDeviceMqttClient(MQTTClient):
    def __init__(self,config):
        try:
            config['client_id'] = ubinascii.hexlify(unique_id())
        except:
            import ubinascii
            from machine import unique_id
            config['client_id'] = ubinascii.hexlify(unique_id())
            
        config['wifi_coro'] = self._on_wifi_change
        config['connect_coro'] = self._on_connect
        config['subs_cb'] = self._on_message 
        
        self._subscriptions = {}
        self._attr_request_dict = {}
        self.__device_on_server_side_rpc_response = None
        self.__connect_callback = None
        self.__device_max_sub_id = 0
        self.__device_client_rpc_number = 0
        self.__device_sub_dict = {}
        self.__device_client_rpc_dict = {}
        self.__attr_request_number = 0
        
        super().__init__(config)

    async def _on_wifi_change(self, state):
        log.info('Wifi is {}'.format('up' if state else 'down'))
        await asyncio.sleep(1)

    async def _on_connect(self, _):
        log.info("connection SUCCESS")
        log.info("Subscribing to topics...")
        await self.subscribe(ATTRIBUTES_TOPIC, qos=1)
        log.info("Subscribed to topic %s successfully!" %(ATTRIBUTES_TOPIC))
        await self.subscribe(ATTRIBUTES_TOPIC + "/response/+", qos=1)
        log.info("Subscribed to topic %s successfully!" %(ATTRIBUTES_TOPIC + "/response/+"))
        await self.subscribe(RPC_REQUEST_TOPIC + '+')
        log.info("Subscribed to topic %s successfully!" %(RPC_REQUEST_TOPIC + '+'))
        await self.subscribe(RPC_RESPONSE_TOPIC + '+', qos=1)
        log.info("Subscribed to topic %s successfully!" %(RPC_RESPONSE_TOPIC + '+'))

    async def _on_message(self, topic, msg, retained):
        decoded = await self._decode(msg)
        await self._on_decoded_message(topic, decoded) 
              
    @staticmethod
    async def _decode(msg):
        decoded = await loads(msg.decode("utf-8"))
        log.debug("_decode:  origional msg: %s" %str(msg))
        log.debug("_decode:  decoded   msg: %s" %str(decoded))
        return decoded

    async def _on_decoded_message(self, topic, decoded):
        if topic.startswith(RPC_REQUEST_TOPIC):
            request_id = topic[len(RPC_REQUEST_TOPIC):len(topic)]
            if self.__device_on_server_side_rpc_response:
                self.__device_on_server_side_rpc_response(request_id, decoded)
        elif topic.startswith(RPC_RESPONSE_TOPIC):
            async with self.lock:
                request_id = int(topic[len(RPC_RESPONSE_TOPIC):len(topic)])
                self.__device_client_rpc_dict.pop(request_id)(request_id, decoded, None)
        elif topic == ATTRIBUTES_TOPIC:
            async with self.lock:
                # callbacks for everything
                if self.__device_sub_dict.get("*"):
                    for x in self.__device_sub_dict["*"]:
                        self.__device_sub_dict["*"][x](decoded, None)
                # specific callback
                keys = decoded.keys()
                keys_list = []
                for key in keys:
                    keys_list.append(key)
                # iterate through msg
                for key in keys_list:
                    # find key in our dict
                    if self.__device_sub_dict.get(key):
                        for x in self.__device_sub_dict[key]:
                            self.__device_sub_dict[key][x](decoded, None)
        elif topic.startswith(ATTRIBUTES_TOPIC_RESPONSE):
            async with self.lock:
                req_id = int(topic[len(ATTRIBUTES_TOPIC+"/response/"):])
                # pop callback and use it
                self._attr_request_dict.pop(req_id)(decoded, None)

    async def _on_publish(self, topic, msg, success):
        if success:
            log.info('Message published to thingsboard successfully!\n topic: {}\n msg: {}'.format(topic,msg))
        else:
            log.info('ERROR: Message publish failure!\n topic: {}\n msg: {}'.format(topic, msg))
        await asyncio.sleep(0)

    # Override _publish method to provide the chance to execute a callback function on publish
    async def _publish(self, topic, msg, retain, qos, dup, pid):
        # Call the original _publish method
        try: 
            await super()._publish(topic, msg, retain, qos, dup, pid)
            success = True
        except: success = False
        # Execute _on_publish callback function
        await self._on_publish(topic, msg, success)

    # def reconnect_delay_set(self, min_delay=1, max_delay=120):
    #     """The client will automatically retry connection. Between each attempt it will wait a number of seconds
    #      between min_delay and max_delay. When the connection is lost, initially the reconnection attempt is delayed
    #      of min_delay seconds. It’s doubled between subsequent attempt up to max_delay. The delay is reset to min_delay
    #       when the connection complete (e.g. the CONNACK is received, not just the TCP connection is established)."""
    #     self.reconnect_delay_set(min_delay, max_delay)


    async def send_rpc_reply(self, req_id, resp, qos=1):
        if qos != 0 and qos != 1:
            log.error("Quality of service (qos) value must be 0 or 1")
            return
        return await self.publish(topic=RPC_RESPONSE_TOPIC + req_id, 
                                msg=resp, 
                                retain=False, 
                                qos=qos)

    async def send_rpc_call(self, method, params, callback):
        async with self.lock:
            self.__device_client_rpc_number += 1
            self.__device_client_rpc_dict.update({self.__device_client_rpc_number: callback})
            rpc_request_id = self.__device_client_rpc_number
        payload = {"method": method, "params": params}
        return await self.publish(topic=RPC_REQUEST_TOPIC + str(rpc_request_id),
                                msg=dumps(payload),
                                retain=False, 
                                qos=1)

    def set_server_side_rpc_request_handler(self, handler):
        self.__device_on_server_side_rpc_response = handler


    async def publish_data(self, data, topic, qos=1):
        data = dumps(data)
        if qos != 0 and qos != 1:
            log.exception("Quality of service (qos) value must be 0 or 1")
            raise TBQoSException("Quality of service (qos) value must be 0 or 1")
        else:
            return await TBPublishInfo(self.publish(topic=topic, 
                                                    msg=data, 
                                                    retain=False, 
                                                    qos=qos))

    async def send_telemetry(self, telemetry, qos=1):
        if type(telemetry) is not list:
            telemetry = [telemetry]
        return await self.publish_data(telemetry, TELEMETRY_TOPIC, qos)


    async def send_attributes(self, attributes, qos=1):
        return await self.publish_data(attributes, ATTRIBUTES_TOPIC, qos)

    def unsubscribe_from_attribute(self, subscription_id):
        async with self.lock:
            for x in self.__device_sub_dict:
                if self.__device_sub_dict[x].get(subscription_id):
                    del self.__device_sub_dict[x][subscription_id]
                    log.info("Unsubscribed from {attribute}, subscription id {sub_id}".format(attribute=x,
                                                                                               sub_id=subscription_id))
            self.__device_sub_dict = dict((k, v) for k, v in self.__device_sub_dict.items() if v is not {})

    async def subscribe_to_all_attributes(self, callback):
        return await self.subscribe_to_attribute("*", callback)

    async def subscribe_to_attribute(self, key, callback):
        async with self.lock:
            self.__device_max_sub_id += 1
            if key not in self.__device_sub_dict:
                self.__device_sub_dict.update({key: {self.__device_max_sub_id: callback}})
            else:
                self.__device_sub_dict[key].update({self.__device_max_sub_id: callback})
            log.debug("Subscribed to {key} with id {id}".format(key=key, id=self.__device_max_sub_id))
            return self.__device_max_sub_id


    async def request_attributes(self, client_keys=None, shared_keys=None, callback=None):
        if client_keys is None and shared_keys is None:
            log.error("There are no keys to request")
            return False
        msg = {}
        if client_keys:
            tmp = ""
            for key in client_keys:
                tmp += key + ","
            tmp = tmp[:len(tmp) - 1]
            msg.update({"clientKeys": tmp})
        if shared_keys:
            tmp = ""
            for key in shared_keys:
                tmp += key + ","
            tmp = tmp[:len(tmp) - 1]
            msg.update({"sharedKeys": tmp})

        ts_in_millis = int(round(time.time() * 1000))
        attr_request_number = await self._add_attr_request_callback(callback)

        return await self.publish(topic=ATTRIBUTES_TOPIC_REQUEST + str(self.__attr_request_number),
                                payload=dumps(msg),
                                retain=False, 
                                qos=1)


    async def _add_attr_request_callback(self, callback):
        async with self.lock:
            self.__attr_request_number += 1
            self._attr_request_dict.update({self.__attr_request_number: callback})
        return self.__attr_request_number
