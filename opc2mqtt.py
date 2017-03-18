import time
import json
import ssl
import paho.mqtt.client as paho
import paho.mqtt as mqtt
import OpenOPC
from collections import OrderedDict


def do_publish(client):
    """Internal function"""
    m = client._userdata.pop()
    if type(m) is dict:
        topic = m['topic']
        try:
            payload = m['payload']
        except KeyError:
            payload = None
        try:
            qos = m['qos']
        except KeyError:
            qos = 1
        try:
            retain = m['retain']
        except KeyError:
            retain = False
    elif type(m) is tuple:
        (topic, payload, qos, retain) = m
    else:
        raise ValueError('message must be a dict or a tuple')
    client.publish(topic, payload, qos, retain)


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        do_publish(client)
    else:
        raise mqtt.MQTTException(paho.connack_string(rc))


# The callback for when message that was to be sent using the publish() call has completed transmission to the broker.
def on_publish(client, userdata, mid):
    if len(userdata) == 0:
        client.disconnect()
    else:
        do_publish(client)


# The callback for when the client disconnects from the broker.
def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("on_disconnect: Unexpected disconnection")


def connectToOPCServer(opcHostName, opcServerName, mode):
    # connect to remote OpenOPC gateway
    if mode == "open":
        try:
            opc = OpenOPC.open_client(opcHostName)
        except:
            print "Unknown host"
    elif mode == "dcom":
        try:
            opc = OpenOPC.client(opcHostName)
        except:
            print "Unknown host"
    else:
        print "Unknown connection mode"

    # Connect to the OPC server inside Gateway
    try:
        opc.connect(opcServerName)
        print "Connected to OpenOPC server " + opcServerName + " on host " + opcHostName
    except:
        print "Unknown OPC Server"
    return opc


def opccreatereadlist(opcConnection, mask):
    return opcConnection.list(mask, flat=True)


def opcJsonPayload(opcConnection, opcReadList, spec):
    payload = ""
    if spec == "tekon_water":
        for name, value, quality, dtime in opcConnection.read(opcReadList):
            # Convert OpenOPC datetime format to ISO8601
            #
            # CONVERT TO UTC
            #
            dtime = time.strftime('%Y-%m-%dZ%H:%M:%ST', time.strptime(dtime, "%m/%d/%y %H:%M:%S"))
            # Convert "quality" to true/false
            quality = True if quality == "Good" else False
            # Use OrderedDict to save JSON keys order
            dataJSON = OrderedDict([("_spec", spec), ("value", int(value)), ("quality", quality)])
            fullJSON = OrderedDict([("meterDescription", name), ("recievedDate", dtime), ("data", dataJSON)])
            payload += json.dumps(fullJSON, indent=4)
    else:
        print "Unknown specification"
    return payload


opc_host, opc_server, mqtt_broker_host, mqtt_client_id = [line.strip() for line in open("settings", 'r').readlines()]
# The loop for periodical read and publish
updateRate = 5
topic = "odintcovo/water"

while True:
    # Create connection to OPC server and read vars
    opcConnection = connectToOPCServer(opc_host, opc_server, "open")
    opcReadList = opccreatereadlist(opcConnection, 'Random.*Int*')
    payload = opcJsonPayload(opcConnection, opcReadList, "tekon_water")

    msg = {'topic': topic, 'payload': payload, 'qos': 1}
    mqttc = paho.Client(client_id=mqtt_client_id, userdata=[msg])
    mqttc.on_connect = on_connect
    mqttc.on_publish = on_publish
    mqttc.on_disconnect = on_disconnect
    mqttc.tls_set(ca_certs='ca.crt', certfile='client_cert.pem', keyfile='client_key.pem',
                  tls_version=ssl.PROTOCOL_TLSv1_2)
    mqttc.tls_insecure_set(True)  # prevents ssl.SSLError: Certificate subject does not match remote hostname.
    mqttc.connect(mqtt_broker_host, port=8883, keepalive=10)
    mqttc.loop_forever()

    opcConnection.close()
    print "Connection to OpenOPC server " + opc_server + " on host " + opc_host + " is closed"
    time.sleep(updateRate)
