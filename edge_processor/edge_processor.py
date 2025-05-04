import pandas as pd
import joblib
import paho.mqtt.client as mqtt
import json
from datetime import datetime

# --- Load Model and Label Mappings ---
MODEL_PATH = "motor_model_xgb.joblib"
MAPPINGS_PATH = "label_mapping_xgb.joblib"

model = joblib.load(MODEL_PATH)
mappings = joblib.load(MAPPINGS_PATH)
label_mapping = mappings["label_mapping"]
anomalie_mapping = mappings["anomalie_mapping"]

print("✅ XGBoost model and mappings loaded.")

# --- MQTT Configuration ---
MQTT_BROKER = "mosquitto"  # Docker container name or broker IP
MQTT_PORT = 1883
MQTT_TOPIC_SUB = "sensors/motor"
# MQTT_TOPIC_PUB = "predictions/motor"  # Uncomment if you want to publish

client = mqtt.Client()

# --- Prediction Function ---
def predict_motor_status(temp, vib, courant, vitesse, timestamp=None):
    if timestamp is None:
        timestamp = datetime.now()
    else:
        timestamp = pd.to_datetime(timestamp)

    hour = timestamp.hour
    dayofweek = timestamp.dayofweek
    month = timestamp.month
    day_night = 1 if 6 <= hour < 18 else 0

    # Prepare input
    input_columns = ["vibration", "temperature", "courant", "vitesse", "hour", "dayofweek", "month", "day_night"]
    input_data = pd.DataFrame([[vib, temp, courant, vitesse, hour, dayofweek, month, day_night]], columns=input_columns)

    # Predict
    pred_code = model.predict(input_data)[0]
    combined_label = label_mapping[pred_code]

    # Decode combined label
    etat_code, anomalie_code = combined_label.split("_")
    etat_moteur = "anormal" if etat_code == "1" else "normal"
    anomalie = anomalie_mapping.get(int(anomalie_code), "inconnue")

    return etat_moteur, anomalie

# --- MQTT Callbacks ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker")
        client.subscribe(MQTT_TOPIC_SUB)
    else:
        print(f"❌ Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print(f"📩 Received sensor data: {payload}")

        # Extract sensor readings
        temperature = payload.get("temperature")
        vibration = payload.get("vibration")
        courant = payload.get("courant")
        vitesse = payload.get("vitesse")
        timestamp = payload.get("timestamp", None)

        # Make prediction
        etat_moteur, anomalie = predict_motor_status(
            temp=temperature,
            vib=vibration,
            courant=courant,
            vitesse=vitesse,
            timestamp=timestamp
        )

        # Build prediction payload
        prediction_payload = {
            "timestamp": timestamp if timestamp else datetime.now().isoformat(),
            "etat_moteur": etat_moteur,
            "anomalie": anomalie
        }

        # Publish prediction if needed
        # client.publish(MQTT_TOPIC_PUB, json.dumps(prediction_payload))

        # Print prediction
        print(f"🚀 Prediction: {prediction_payload}")

    except Exception as e:
        print(f"⚠️ Error processing message: {e}")

# --- Start MQTT Client ---
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT)
client.loop_forever()
