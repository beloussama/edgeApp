import random
import time
import json
import socket
from datetime import datetime
from pymongo import MongoClient
import paho.mqtt.publish as publish

# --- MQTT Config ---
BROKER = "mosquitto"
PORT = 1883
TOPIC = "sensors/motor"

# --- MongoDB Config ---
MONGO_URI = "put ur link to mongo db"
client = MongoClient(MONGO_URI)
db = client["motor_data"]
collection = db["sensor_data"]

# --- États de fonctionnement ---
etat_courant = "normal"
compteur_cycles = 0
anomalie_active = None

# --- Liste des scénarios d'anomalies ---
anomalies_possibles = [
    "palier_defaillant",
    "frein_bloque",
    "surchauffe_stator",
    "grippage_mecanique"
]

def evolution_etat():
    global etat_courant, compteur_cycles, anomalie_active
    compteur_cycles += 1

    if etat_courant == "normal":
        if compteur_cycles > 3 and random.random() < 0.5:
            etat_courant = "anormal"
            anomalie_active = random.choices(
                anomalies_possibles,
                weights=[3, 2, 2, 3],
                k=1
            )[0]
            compteur_cycles = 0
    elif etat_courant == "anormal":
        if compteur_cycles > random.randint(5, 10):
            etat_courant = "normal"
            anomalie_active = None
            compteur_cycles = 0

def generer_donnees():
    timestamp = datetime.utcnow().isoformat()
    # --- Valeurs normales pour un moteur industriel ---
    vibration = round(random.uniform(0.1, 0.5), 2)        # g
    temperature = round(random.uniform(45, 65), 2)        # °C
    courant = round(random.uniform(6, 10), 2)             # A
    vitesse = round(random.uniform(1450, 1500), 2)        # tr/min

    # --- Ajustement selon l'anomalie active ---
    if etat_courant == "anormal":
        if anomalie_active == "palier_defaillant":
            vibration = round(random.uniform(2.0, 3.5), 2)
            courant += random.uniform(3, 5)
            temperature += random.uniform(10, 15)

        elif anomalie_active == "frein_bloque":
            vitesse = round(random.uniform(1100, 1300), 2)
            courant += random.uniform(5, 8)
            temperature += random.uniform(20, 30)
            vibration += random.uniform(0.3, 0.7)

        elif anomalie_active == "surchauffe_stator":
            temperature = round(random.uniform(90, 120), 2)
            courant += random.uniform(4, 7)
            if random.random() < 0.5:
                vitesse -= random.uniform(50, 150)
            vibration += random.uniform(0.1, 0.4)

        elif anomalie_active == "grippage_mecanique":
            vibration = round(random.uniform(2.0, 4.0), 2)
            vitesse = round(random.uniform(1250, 1350), 2)
            temperature += random.uniform(15, 25)
            courant += random.uniform(3, 5)

    return {
        "timestamp": timestamp,
        "etat_moteur": etat_courant,
        "anomalie": anomalie_active if etat_courant != "normal" else None,
        "vibration": round(vibration, 2),
        "temperature": round(temperature, 2),
        "courant": round(courant, 2),
        "vitesse": round(vitesse, 2),
    }

# --- MQTT Broker Wait Function ---
def wait_for_broker(host, port, timeout=30):
    for _ in range(timeout):
        try:
            with socket.create_connection((host, port), timeout=2):
                print("[MQTT] Broker is ready.")
                return True
        except OSError:
            print("[MQTT] Waiting for broker...")
            time.sleep(1)
    return False

# --- Main Loop ---
if wait_for_broker(BROKER, PORT):
    try:
        print("Simulation moteur avec publication MQTT...")

        while True:
            evolution_etat()
            data = generer_donnees()

            # Affichage
            print(f"[{data['timestamp']}] {data['etat_moteur'].upper()} | Anomalie: {data['anomalie']} | Données: {data}")

            # MongoDB
            collection.insert_one(data)

            data.pop("_id", None)

            # MQTT Publish
            try:
                publish.single(TOPIC, payload=json.dumps(data), hostname=BROKER)
            except Exception as e:
                print(f"[MQTT] Publish error: {e}")

            time.sleep(2)

    except KeyboardInterrupt:
        print("Simulation arrêtée.")
else:
    print("[MQTT] Le broker n'est pas disponible.")
