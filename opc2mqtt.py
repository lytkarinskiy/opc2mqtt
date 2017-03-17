import time
import json
import OpenOPC
import ssl
from collections import OrderedDict
import paho.mqtt.client as mqtt

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	msg = ""
	if rc == 0:
		msg = "Connection successful" 
	elif rc == 1:
		msg = "Connection refused - incorrect protocol version"
	elif rc == 2:
		msg = "Connection refused - invalid client identifier"
	elif rc == 3:
		msg = "Connection refused - server unavailable"
	elif rc == 4:
		msg = "Connection refused - bad username or password"
	elif rc == 5:
		msg = "Connection refused - not authorised" 
	else:
		msg = "Unknown error"
	print "on_connect: " + str(msg)

# The callback for when message that was to be sent using the publish() call has completed transmission to the broker.
def on_publish(client, userdata, mid):
    print "on_publish: mid "+str(mid)

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
topic = "odintcovo/water"
qos=1
hostname=hostname
port=8883
client_id=client_id

debug = False
mqttc = mqtt.Client(client_id)
if debug == True:
	mqttc.on_connect = on_connect
	mqttc.on_publish = on_publish
	mqttc.on_disconnect = on_disconnect
mqttc.tls_set(ca_certs= homedir + 'ca.crt', certfile= homedir + 'client_cert.pem', keyfile= homedir + 'client_key.pem', tls_version=ssl.PROTOCOL_TLSv1_2)
mqttc.tls_insecure_set(True)     # prevents error - ssl.SSLError: Certificate subject does not match remote hostname.
mqttc.connect(hostname, port)
mqttc.loop_start()

# The loop for periodical read and publish
updateRate = 3
while True:
	for name, value, quality, dtime in opc.read(readList):
		# Convert OpenOPC datetime format to ISO8601
		dtime = time.strftime('%Y-%m-%dZ%H:%M:%ST', time.strptime(dtime, "%m/%d/%y %H:%M:%S"))
		# Convert "quality" to true/false
		quality = True if quality=="Good" else False
		# Use OrderedDict to save JSON keys order
		dataJSON = OrderedDict([("_spec", "tekon_water"), ("value", int(value)),("quality", quality)])
		fullJSON = OrderedDict([("meterDescription", name),("recievedDate", dtime),("data", dataJSON)])
		payload = json.dumps(fullJSON, indent = 4)
		# print payload
	mqttc.publish(topic, payload, qos)
	time.sleep(updateRate)

opc.close()
print "OPC closed"
mqttc.loop_stop()
print "loop stopped"
mqttc.disconnect()
print "MQTT disconnected"