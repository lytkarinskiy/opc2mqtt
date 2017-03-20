import datetime
import json
import ssl
import re
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

def opc_connect(hostname, opc_server, mode="open"):
    # connect to remote OpenOPC gateway
    if mode == "open":
        try:
            opc = OpenOPC.open_client(hostname)
        except:
            print "Unknown open_opc host"
    elif mode == "dcom":
        try:
            opc = OpenOPC.client(hostname)
        except:
            print "Unknown dcom host"
    else:
        print "Unknown connection mode"

    # Connect to the OPC server inside Gateway
    try:
        opc.connect(opc_server)
        print "Connected to OpenOPC server " + opc_server + " on host " + hostname
    except:
        print "Unknown OPC Server"
    return opc

def opc_create_read_list(opc_connection, mask):
    return opc_connection.list(mask, flat=True)

def json_payload(connection, read_list, spec):
    payload = ""
    if spec == "tekon_water":
        for name, value, quality, dtime in connection.read(read_list):
            if quality != "Good":
            	# Use and convert current datetime to UTC ISO8601
                value = 0
                dtime = datetime.datetime.utcnow().strftime('%Y-%m-%dZ%H:%M:%ST')
            else:
                # Use and convert received datetime to UTC ISO8601
                dtime = datetime.datetime.strptime(dtime, "%m/%d/%y %H:%M:%S")
                # Shift hour to UTC
                dtime = dtime - datetime.timedelta(hours = 3)
                dtime = dtime.strftime('%Y-%m-%dZ%H:%M:%ST')
            # Convert "quality" to true/false
            quality = True if quality == "Good" else False
            # Replace russian in name
            name = re.sub(r'USB.*- ', 'USB_Pult.KIR-', name, re.DOTALL)
            # Use OrderedDict to save JSON keys order
            data = OrderedDict([("_spec", spec), ("value", int(value)), ("quality", quality)])
            full = OrderedDict([("meterDescription", name), ("receivedDate", dtime), ("data", data)])
            # Dump payload to JSON
            payload += json.dumps(full, indent=4) + "\n"
        print payload
    else:
        print "Unknown specification"
    return payload

# Infinite loop to read and publish with updateRate priod
updateRate = 5
topic = "odintcovo/water"
opc_host, opc_server, mqtt_broker_host, mqtt_client_id = [line.strip() for line in open("settings", 'r').readlines()]

while True:
    # Create connection to OPC server and read vars
    opc_connection = opc_connect(opc_host, opc_server)
    opc_read_list = opc_create_read_list(opc_connection, 'Random.*Int*')
    payload = json_payload(opc_connection, opc_read_list, "tekon_water")

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

    opc_connection.close()
    print "Connection to OpenOPC server " + opc_server + " on host " + opc_host + " is closed"
    time.sleep(updateRate)