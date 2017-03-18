import time
import json
import ssl
import paho.mqtt.client as paho
import paho.mqtt as mqtt
import OpenOPC
from collections import OrderedDict

def do_publish(c):
    """Internal function"""
    m = c._userdata.pop()
    if type(m) is dict:
        topic = m['topic']
        try:
            payload = m['payload']
        except KeyError:
            payload = None
        try:
            qos = m['qos']
        except KeyError:
            qos = 0
        try:
            retain = m['retain']
        except KeyError:
            retain = False
    elif type(m) is tuple:
        (topic, payload, qos, retain) = m
    else:
        raise ValueError('message must be a dict or a tuple')
    c.publish(topic, payload, qos, retain)

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

homedir = '/home/osboxes/Documents/certs/'
opc_host, opc_server, hostname, client_id = [line.strip() for line in open(homedir + "settings").readlines()]

opc_host = opc_host
opc_server = opc_server

# connect to remote OpenOPC gateway
try:
	opc = OpenOPC.open_client(opc_host)
except:
	print "Unknown host"

# Connect to the OPC server inside Gateway
try:
	opc.connect(opc_server)
	print "Connected to OPC server " + opc_server + " on host " + opc_host
except:
	print "Unknown OPC Server"

# Create reding list
readList = opc.list('Random.Int*', flat = True)

# Define MQTT broker/client properties
hostname=hostname
port=8883
client_id=client_id
topic = "odintcovo/water"
qos=1
retain = False



# The loop for periodical read and publish
updateRate = 15
while True:
	payload = ""
	for name, value, quality, dtime in opc.read(readList):
		# Convert OpenOPC datetime format to ISO8601
		# 
		# CONVERT TO UTC
		# 
		dtime = time.strftime('%Y-%m-%dZ%H:%M:%ST', time.strptime(dtime, "%m/%d/%y %H:%M:%S"))
		# Convert "quality" to true/false
		quality = True if quality=="Good" else False
		# Use OrderedDict to save JSON keys order
		dataJSON = OrderedDict([("_spec", "tekon_water"), ("value", int(value)),("quality", quality)])
		fullJSON = OrderedDict([("meterDescription", name),("recievedDate", dtime),("data", dataJSON)])
		payload += json.dumps(fullJSON, indent = 4)
	# print payload
	msg = {'topic':topic, 'payload':payload, 'qos':qos, 'retain':retain}
	# print msg
	mqttc = paho.Client(client_id=client_id, userdata = [msg])
	# print mqttc._userdata
	mqttc.on_connect = on_connect
	mqttc.on_publish = on_publish
	mqttc.on_disconnect = on_disconnect
	mqttc.tls_set(ca_certs = homedir + 'ca.crt', 
		certfile = homedir + 'client_cert.pem', 
		keyfile = homedir + 'client_key.pem', 
		tls_version=ssl.PROTOCOL_TLSv1_2)
	mqttc.tls_insecure_set(True)     # prevents ssl.SSLError: Certificate subject does not match remote hostname.
	mqttc.connect(hostname, port, keepalive = 10)
	mqttc.loop_forever()
	time.sleep(updateRate)
opc.close()
print "OPC closed"