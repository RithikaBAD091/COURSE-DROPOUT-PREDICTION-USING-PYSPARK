import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import joblib
import numpy as np

print("="*60)
print("🎓 DROPOUT PREDICTION MODEL TRAINING")
print("="*60)

# ------------------------------------------------
# 1️⃣ Start Spark Session
# ------------------------------------------------
print("\n📊 Initializing Spark Session...")
spark = (
    SparkSession.builder
    .appName("Dropout Prediction Training")
    .master("local[*]")
    .getOrCreate()
)
print("✅ Spark session started successfully!")

# ------------------------------------------------
# 2️⃣ Load Dataset
# ------------------------------------------------
print("\n📂 Loading datasets...")
try:
    student = spark.read.csv("datasets/studentInfo.csv", header=True, inferSchema=True)
    vle = spark.read.csv("datasets/studentVle.csv", header=True, inferSchema=True)
    assess = spark.read.csv("datasets/studentAssessment.csv", header=True, inferSchema=True)
    print("✅ All datasets loaded successfully!")
except Exception as e:
    print(f"❌ Error loading dataset: {e}")
    exit(1)

# ------------------------------------------------
# 3️⃣ Data Preprocessing
# ------------------------------------------------
print("\n🔧 Preprocessing data...")

# Label: Withdrawn = 1
student = student.withColumn(
    "dropout_label",
    when(col("final_result") == "Withdrawn", 1).otherwise(0)
)

# Engagement Aggregates
vle_agg = vle.groupBy("id_student").agg(
    sum("sum_click").alias("TotalClicks"),
    F.countDistinct("date").alias("ActiveDays")
)

vle_agg = vle_agg.withColumn(
    "AvgClicks",
    when(col("ActiveDays") > 0, col("TotalClicks") / col("ActiveDays")).otherwise(0)
)

# Assessment Aggregates
assess_agg = assess.groupBy("id_student").agg(
    avg("score").alias("AvgScore"),
    sum(when(col("score") < 40, 1).otherwise(0)).alias("FailedAssessments"),
    count("*").alias("TotalAssessments")
)

# Merge all data
df = student.join(vle_agg, "id_student", "left").join(assess_agg, "id_student", "left").fillna(0)

# Convert to pandas
pdf = df.toPandas()
print(f"✅ Data preprocessed! Total records: {len(pdf)}")

# ------------------------------------------------
# 4️⃣ Feature Engineering
# ------------------------------------------------
print("\n🎯 Engineering features...")

# Encode categorical features
pdf['gender_encoded'] = pdf['gender'].map({'M': 1, 'F': 0})

# Age band encoding
age_mapping = {
    '0-35': 0,
    '35-55': 1,
    '55<=': 2
}
pdf['age_encoded'] = pdf['age_band'].map(age_mapping).fillna(0)

# Education level encoding
edu_mapping = {
    'No Formal quals': 0,
    'Lower Than A Level': 1,
    'A Level or Equivalent': 2,
    'HE Qualification': 3,
    'Post Graduate Qualification': 4
}
pdf['edu_encoded'] = pdf['highest_education'].map(edu_mapping).fillna(0)

# Select features
feature_columns = [
    'TotalClicks',
    'ActiveDays',
    'AvgClicks',
    'AvgScore',
    'FailedAssessments',
    'TotalAssessments',
    'gender_encoded',
    'age_encoded',
    'edu_encoded'
]

X = pdf[feature_columns].fillna(0)
y = pdf['dropout_label']

print(f"✅ Features prepared! Shape: {X.shape}")
print(f"📊 Feature distribution:")
print(f"   - Active students: {(y == 0).sum()}")
print(f"   - Dropout students: {(y == 1).sum()}")

# ------------------------------------------------
# 5️⃣ Train-Test Split
# ------------------------------------------------
print("\n✂️ Splitting data into train and test sets...")
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"✅ Train set: {len(X_train)} samples")
print(f"✅ Test set: {len(X_test)} samples")

# ------------------------------------------------
# 6️⃣ Train Random Forest Model
# ------------------------------------------------
print("\n🌲 Training Random Forest Classifier...")
print("⏳ This may take a few moments...")

rf_model = RandomForestClassifier(
    n_estimators=100,
    max_depth=15,
    min_samples_split=10,
    min_samples_leaf=5,
    random_state=42,
    n_jobs=-1,
    verbose=1
)

rf_model.fit(X_train, y_train)
print("✅ Model training completed!")

# ------------------------------------------------
# 7️⃣ Model Evaluation
# ------------------------------------------------
print("\n📈 Evaluating model performance...")

# Predictions
y_pred = rf_model.predict(X_test)

# Accuracy
accuracy = accuracy_score(y_test, y_pred)
print(f"\n🎯 Model Accuracy: {accuracy * 100:.2f}%")

# Classification Report
print("\n📋 Classification Report:")
print(classification_report(y_test, y_pred, target_names=['Active', 'Dropout']))

# Confusion Matrix
print("\n🔢 Confusion Matrix:")
cm = confusion_matrix(y_test, y_pred)
print(cm)

# Feature Importance
print("\n⭐ Feature Importance:")
feature_importance = pd.DataFrame({
    'Feature': feature_columns,
    'Importance': rf_model.feature_importances_
}).sort_values('Importance', ascending=False)
print(feature_importance.to_string(index=False))

# ------------------------------------------------
# 8️⃣ Save Model
# ------------------------------------------------
print("\n💾 Saving trained model...")
joblib.dump(rf_model, 'models/dropout_rf_model.pkl')
print("✅ Model saved to 'models/dropout_rf_model.pkl'")

# Save feature names
with open('models/feature_names.txt', 'w') as f:
    f.write(','.join(feature_columns))
print("✅ Feature names saved to 'models/feature_names.txt'")

# ------------------------------------------------
# 9️⃣ Generate Predictions for Dashboard
# ------------------------------------------------
print("\n🔮 Generating predictions for all students...")
pdf['dropout_prediction'] = rf_model.predict(X)
pdf['dropout_probability'] = rf_model.predict_proba(X)[:, 1]

# Save predictions
pdf.to_csv('datasets/student_predictions.csv', index=False)
print("✅ Predictions saved to 'datasets/student_predictions.csv'")

# ------------------------------------------------
# 🔟 Summary
# ------------------------------------------------
print("\n" + "="*60)
print("✅ TRAINING COMPLETED SUCCESSFULLY!")
print("="*60)
print(f"📊 Total Students Analyzed: {len(pdf)}")
print(f"🎯 Model Accuracy: {accuracy * 100:.2f}%")
print(f"🔴 High Risk Students: {(pdf['dropout_probability'] > 0.7).sum()}")
print(f"🟡 Medium Risk Students: {((pdf['dropout_probability'] > 0.4) & (pdf['dropout_probability'] <= 0.7)).sum()}")
print(f"🟢 Low Risk Students: {(pdf['dropout_probability'] <= 0.4).sum()}")
print("\n🚀 Ready to run the dashboard!")
print("   Run: streamlit run app.py")
print("="*60)

# Stop Spark
spark.stop()
print("\n✅ Spark session stopped.")