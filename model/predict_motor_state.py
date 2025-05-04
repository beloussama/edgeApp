import joblib
import pandas as pd

# Load model and label mapping
model = joblib.load('motor_model.joblib')  # <- Make sure this matches the training script
label_mapping = joblib.load('label_mapping.joblib')
print("✅ Logistic Regression model and label mapping loaded.")

# Define prediction function
def predict_motor_status(vibration, temperature, courant, vitesse, timestamp):
    try:
        ts = pd.to_datetime(timestamp)
        hour = ts.hour
        dayofweek = ts.dayofweek
        month = ts.month
        day_night = 1 if 6 <= hour < 18 else 0

        input_data = pd.DataFrame([{
            "vibration": vibration,
            "temperature": temperature,
            "courant": courant,
            "vitesse": vitesse,
            "hour": hour,
            "dayofweek": dayofweek,
            "month": month,
            "day_night": day_night
        }])

        # Predict
        pred_code = model.predict(input_data)[0]
        combined_label = label_mapping[pred_code]

        # Decode
        etat_code, anomalie_code = combined_label.split("_")
        etat_moteur = "anormal" if etat_code == "1" else "normal"

        anomalies = {
            0: "aucune",
            1: "palier_defaillant",
            2: "frein_bloque",
            3: "surchauffe_stator",
            4: "grippage_mecanique"
        }
        anomalie = anomalies.get(int(anomalie_code), "inconnue")

        return {
            "etat_moteur": etat_moteur,
            "anomalie": anomalie
        }

    except Exception as e:
        return {"error": str(e)}

# Example use
example = predict_motor_status(2.46, 60.94, 11.53, 1489.28, "2025-04-21T22:00:00")
print("🔍 Prediction:", example)
