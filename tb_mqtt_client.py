import paho.mqtt.client as paho
import logging
import time

attributes_url = 'v1/devices/me/attributes'
telemetry_url = 'v1/devices/me/telemetry'
log = logging.getLogger(__name__)


class TbClient:
    class SubscriptionInfo:
        def __init__(self, sub_id, cb):
            self.subscription_id = sub_id
            self.callback = cb

    def __init__(self, host, token):
        self.client = paho.Client()
        self.host = host
        self.client.username_pw_set(token)
        self.is_connected = False
        self.sub_dict = {}

        def on_log(client, userdata, level, buf):
            log.debug(buf)

        def on_connect(client, userdata, flags, rc, *extra_params):
            result_codes = {
                1: "incorrect protocol version",
                2: "invalid client identifier",
                3: "server unavailable",
                4: "bad username or password",
                5: "not authorised",
            }
            if self.__connect_callback:
                self.__connect_callback(client, userdata, flags, rc, *extra_params)
                #TODO какие параметры нужно передавать?
            if rc == 0:
                self.is_connected = True
                log.info("connection SUCCESS")
            else:
                if rc in result_codes:
                    log.error("connection FAIL with error '%i':'%s'" % (rc, result_codes[rc]))
                else:
                    log.error("connection FAIL with unknown error")

        def on_disconnect(client, userdata, rc):
            self.is_connected = False
            if self.__disconnect_callback:
                #TODO нужно передавать юзердату?
                self.__disconnect_callback(userdata, rc)
            if rc == 0:
                log.info("disconnect SUCCESS")
            else:
                log.error("disconnect FAIL with error code %i" % rc)

        def on_publish(client, userdata, result):
            log.info("data published")

        def on_message(client, userdata, message):
            content = message.payload.decode("utf-8")
            log.info(content)
            log.info(message.topic)
            if message.topic == attributes_url:
                message = eval(content)
                for key in self.sub_dict.keys():
                    if self.sub_dict.get(key):
                        for item in self.sub_dict.get(key):
                            item.callback(message)

        self.client.on_disconnect = on_disconnect
        self.client.on_connect = on_connect
        self.client.on_log = on_log
        self.client.on_publish = on_publish
        self.client.on_message = on_message

    def __connect_callback(self, *args):
        pass

    def connect(self, callback=None, timeout=10):
        self.client.connect(self.host)
        self.client.loop_start()
        self.__connect_callback = callback
        t = time.time()
        self.is_connected = 0
        while self.is_connected is not True:
            time.sleep(0.2)
            if time.time()-t > timeout:
                return False
        return True

    def disconnect(self):
        self.client.disconnect()
        log.info("DISCONNECT")

    def __disconnect_callback(self, *args):
        pass

    def send_telemetry(self, telemetry, quality_of_service=0, blocking=False):
        info = self.client.publish(telemetry_url, telemetry, quality_of_service)
        if blocking:
            info.wait_for_publish()

    def send_attributes(self, attributes, quality_of_service=0, blocking=False):
        info = self.client.publish(attributes_url, attributes, quality_of_service)
        if blocking:
            info.wait_for_publish()

    def unsubscribe(self, subscription_id):
        empty_keys = []
        for attribute in self.sub_dict.keys():
            for x in self.sub_dict[attribute]:
                if x.subscription_id == subscription_id:
                    self.sub_dict[attribute].remove(x)
                    log.debug("Unsubscribed from " + attribute + ". subscription id " + str(subscription_id))
            if not self.sub_dict[attribute]:
                empty_keys.append(attribute)

        for key in empty_keys:
            del self.sub_dict[key]

    def subscribe(self, callback=None, key="*", quality_of_service=2, to_rpc=False):

        if to_rpc:
            self.client.subscribe('v1/devices/me/rpc/request/')
            return True

        self.client.subscribe(attributes_url, qos=quality_of_service)

        def find_max_sub_id():
            res = 1
            for attrib in self.sub_dict.keys():
                for item in self.sub_dict[attrib]:
                    if item.subscription_id > res:
                        res = item.subscription_id
            return res

        inst = self.SubscriptionInfo(find_max_sub_id(), callback)

        # subscribe to everything
        if key == "*":
            for attr in self.sub_dict.keys():
                if inst not in self.sub_dict[attr]:
                    self.sub_dict[attr].append(inst)
                    log.debug("Subscribed to " + attr + ", subscription id " + str(inst.subscription_id))
        # if attribute doesnot exist create it with subscription
        elif key not in self.sub_dict.keys():
            self.sub_dict.update({key: [inst]})
            log.debug("Subscribed to " + key + ", subscription id " + str(inst.subscription_id))
        # if attribute exists create subscription
        else:
            self.sub_dict[key].append(inst)
            log.debug("Subscribed to " + key + ", subscription id " + str(inst.subscription_id))
        return inst.subscription_id
