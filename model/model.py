from pymongo import MongoClient
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from xgboost import XGBClassifier
import joblib

# --- Connect to MongoDB ---
client = MongoClient("mongodb+srv://put ur link to mongo db")
collection = client["motor_data"]["sensor_data"]

# --- Load and preprocess data ---
data = pd.DataFrame(list(collection.find()))
data.drop(columns=['_id'], inplace=True)

# Feature engineering
data['timestamp'] = pd.to_datetime(data['timestamp'])
data['hour'] = data['timestamp'].dt.hour
data['dayofweek'] = data['timestamp'].dt.dayofweek
data['month'] = data['timestamp'].dt.month
data['day_night'] = data['hour'].apply(lambda x: 1 if 6 <= x < 18 else 0)

# Handle missing anomalies
data['anomalie'] = data['anomalie'].fillna("aucune")

# Encode labels
# --- Encode etat_moteur (normal=0, anormal=1)
data['etat_moteur'] = data['etat_moteur'].map({"normal": 0, "anormal": 1})

# --- Encode anomalies
data['anomalie_code'] = data['anomalie'].astype("category").cat.codes
anomalie_mapping = dict(enumerate(data['anomalie'].astype("category").cat.categories))

# --- Create combined label
data['combined_label'] = data['etat_moteur'].astype(str) + "_" + data['anomalie_code'].astype(str)
data['combined_label_code'] = data['combined_label'].astype("category").cat.codes
label_mapping = dict(enumerate(data['combined_label'].astype("category").cat.categories))

# Features and Target
features = ['vibration', 'temperature', 'courant', 'vitesse', 'hour', 'dayofweek', 'month', 'day_night']
X = data[features]
y = data['combined_label_code']

# --- Split data ---
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# --- Train XGBoost Model ---
xgb_model = XGBClassifier(
    objective='multi:softmax',
    num_class=len(np.unique(y)),
    eval_metric='mlogloss',
    use_label_encoder=False,
    random_state=42,
    max_depth=6,
    n_estimators=100,
    learning_rate=0.1
)

xgb_model.fit(X_train, y_train)

# --- Save model and mappings ---
joblib.dump(xgb_model, 'motor_model_xgb.joblib')
joblib.dump({
    "label_mapping": label_mapping,
    "anomalie_mapping": anomalie_mapping
}, 'label_mapping_xgb.joblib')

# --- Evaluate model ---
y_pred = xgb_model.predict(X_test)
acc = accuracy_score(y_test, y_pred)

print(f"✅ XGBoost model saved successfully. Accuracy: {acc:.4f}")
print(classification_report(y_test, y_pred, target_names=list(label_mapping.values())))
