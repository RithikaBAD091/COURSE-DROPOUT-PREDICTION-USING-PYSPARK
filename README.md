# COURSE-DROPOUT-PREDICTION-USING-PYSPARK
This project analyzes student engagement, demographics, and performance patterns in Massive Open Online Courses (MOOCs) to predict the likelihood of a student dropping out.
Using PySpark, we process large-scale educational datasets and build an ML model that identifies at-risk learners early—helping improve student success and course retention.

Problem Identification

MOOC platforms face several challenges:

Extremely high dropout rates across online courses.
Student interaction data is large, complex, and unstructured.
Instructors do not have automated early warning systems.
Manual tracking of thousands of learners is impossible.

This project solves these issues using data-driven predictions.
Tech Stack

Programming
Python
PySpark (Spark MLlib)

Framework
Apache Spark (MLlib Classification Models)
Data Processing
Spark SQL
DataFrames
Feature Engineering
Storage
CSV Files (OULAD Dataset)

Visualization
Matplotlib
Seaborn

Dataset Source

Open University Learning Analytics Dataset (OULAD)

Files Used

studentInfo.csv — demographics + dropout label

studentVle.csv — clickstream interactions

studentAssessment.csv — assignment & quiz performance

Format: CSV (comma-separated values)

Dataset Preprocessing

Data preparation steps include:

Merged student info, VLE activity logs, and assessment results

Removed duplicates and handled missing values

Converted final_result → Dropout_Label

1 = Withdrawn

0 = Completed

Feature Engineering

Engagement Features

total clicks

active days

average clicks

Performance Features

average quiz score

failed attempts

Demographic Features

age group

gender

education level

disability status

Machine Learning Approach

Using Spark MLlib classification algorithms:

Logistic Regression

Random Forest Classifier

Gradient Boosted Trees

Decision Trees

Pipeline Includes

VectorAssembler

StandardScaler

Train–Test Split

Model Training

Evaluation Metrics

Expected Outcomes

Dropout Risk Score for each student

Binary prediction → Continue or Withdraw

Performance Metrics

Accuracy

Precision / Recall

ROC-AUC

Impact

Helps instructors identify at-risk students early

Enables personalized interventions

Improves retention in online learning platforms

Spark Web UI

The project uses the Spark Web UI to:

visualize job execution timelines

track stages and tasks

monitor cluster performance

measure processing time

debug slow transformations in pipelines

This enhances transparency of model training and large-scale data processing.

Visualizations Included

Student engagement distributions

Correlation between activity & dropout

Assessment score trends

Dropout comparison across demographics

How to Run the Project
# 1. Start Spark Session
# 2. Load OULAD CSV files
# 3. Run preprocessing scripts
# 4. Train ML model using MLlib
# 5. View performance & predictions


Ensure PySpark is properly installed and configured.
