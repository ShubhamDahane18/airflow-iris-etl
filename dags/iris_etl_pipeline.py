# # airflow-iris-etl\dags

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# import pandas as pd
# import sqlite3
# import os
# from sklearn.datasets import load_iris
# import numpy as np

# # Default arguments for the DAG
# default_args = {
#     'owner': 'data_engineer',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Define paths
# DATA_DIR = '/tmp/airflow_data'
# RAW_DATA_PATH = f'{DATA_DIR}/iris_raw.csv'
# PROCESSED_DATA_PATH = f'{DATA_DIR}/iris_processed.csv'
# DB_PATH = f'{DATA_DIR}/iris.db'


# def extract_iris_data():
#     """Extract: Load Iris dataset from sklearn"""
#     os.makedirs(DATA_DIR, exist_ok=True)
    
#     # Load iris dataset
#     iris = load_iris()
    
#     # Create DataFrame
#     df = pd.DataFrame(
#         data=iris.data,
#         columns=iris.feature_names
#     )
#     df['species'] = iris.target
#     df['species_name'] = df['species'].map({
#         0: 'setosa',
#         1: 'versicolor', 
#         2: 'virginica'
#     })
    
#     # Save raw data
#     df.to_csv(RAW_DATA_PATH, index=False)
#     print(f"✓ Iris dataset extracted: {len(df)} records")
#     print(f"✓ Features: {list(iris.feature_names)}")
#     print(f"✓ Species: {df['species_name'].unique().tolist()}")


# def transform_iris_data():
#     """Transform: Clean and engineer features"""
#     # Read raw data
#     df = pd.read_csv(RAW_DATA_PATH)
    
#     # Create new features
#     # 1. Petal area
#     df['petal_area'] = df['petal length (cm)'] * df['petal width (cm)']
    
#     # 2. Sepal area
#     df['sepal_area'] = df['sepal length (cm)'] * df['sepal width (cm)']
    
#     # 3. Petal to sepal ratio
#     df['petal_sepal_ratio'] = df['petal_area'] / df['sepal_area']
    
#     # 4. Size category based on petal length
#     df['size_category'] = pd.cut(
#         df['petal length (cm)'],
#         bins=[0, 2, 5, 7],
#         labels=['Small', 'Medium', 'Large']
#     )
    
#     # 5. Add quality score (weighted combination of measurements)
#     df['quality_score'] = (
#         df['petal length (cm)'] * 0.3 +
#         df['petal width (cm)'] * 0.2 +
#         df['sepal length (cm)'] * 0.3 +
#         df['sepal width (cm)'] * 0.2
#     ).round(2)
    
#     # Statistical features
#     df['total_length'] = df['petal length (cm)'] + df['sepal length (cm)']
#     df['total_width'] = df['petal width (cm)'] + df['sepal width (cm)']
    
#     # Save processed data
#     df.to_csv(PROCESSED_DATA_PATH, index=False)
#     print(f"✓ Data transformed successfully")
#     print(f"✓ New features added: petal_area, sepal_area, petal_sepal_ratio")
#     print(f"✓ Total columns: {len(df.columns)}")
#     print(f"\nSample transformed data:")
#     print(df.head(3))


# def load_to_database():
#     """Load: Insert data into SQLite database"""
#     # Read processed data
#     df = pd.read_csv(PROCESSED_DATA_PATH)
    
#     # Connect to SQLite database
#     conn = sqlite3.connect(DB_PATH)
    
#     # Load data to database
#     df.to_sql('iris_data', conn, if_exists='replace', index=False)
    
#     # Create summary table
#     summary_query = """
#     CREATE TABLE IF NOT EXISTS species_summary AS
#     SELECT 
#         species_name,
#         COUNT(*) as sample_count,
#         ROUND(AVG("petal length (cm)"), 2) as avg_petal_length,
#         ROUND(AVG("petal width (cm)"), 2) as avg_petal_width,
#         ROUND(AVG("sepal length (cm)"), 2) as avg_sepal_length,
#         ROUND(AVG("sepal width (cm)"), 2) as avg_sepal_width,
#         ROUND(AVG(quality_score), 2) as avg_quality_score
#     FROM iris_data
#     GROUP BY species_name
#     """
    
#     conn.execute(summary_query)
#     conn.commit()
    
#     # Verify data
#     cursor = conn.cursor()
#     cursor.execute("SELECT COUNT(*) FROM iris_data")
#     count = cursor.fetchone()[0]
    
#     print(f"✓ Data loaded to database: {count} records")
#     print(f"✓ Tables created: iris_data, species_summary")
    
#     conn.close()


# def generate_analysis_report():
#     """Generate comprehensive analysis report"""
#     conn = sqlite3.connect(DB_PATH)
    
#     print("\n" + "="*60)
#     print("IRIS DATASET ANALYSIS REPORT")
#     print("="*60)
    
#     # 1. Species Summary
#     print("\n📊 SPECIES SUMMARY:")
#     print("-" * 60)
#     query_species = """
#     SELECT * FROM species_summary
#     ORDER BY avg_quality_score DESC
#     """
#     df_species = pd.read_sql_query(query_species, conn)
#     print(df_species.to_string(index=False))
    
#     # 2. Size Distribution
#     print("\n📏 SIZE DISTRIBUTION BY SPECIES:")
#     print("-" * 60)
#     query_size = """
#     SELECT 
#         species_name,
#         size_category,
#         COUNT(*) as count
#     FROM iris_data
#     GROUP BY species_name, size_category
#     ORDER BY species_name, size_category
#     """
#     df_size = pd.read_sql_query(query_size, conn)
#     print(df_size.to_string(index=False))
    
#     # 3. Top Quality Samples
#     print("\n🏆 TOP 5 QUALITY SAMPLES:")
#     print("-" * 60)
#     query_top = """
#     SELECT 
#         species_name,
#         ROUND("petal length (cm)", 2) as petal_length,
#         ROUND("sepal length (cm)", 2) as sepal_length,
#         quality_score,
#         size_category
#     FROM iris_data
#     ORDER BY quality_score DESC
#     LIMIT 5
#     """
#     df_top = pd.read_sql_query(query_top, conn)
#     print(df_top.to_string(index=False))
    
#     # 4. Statistical Insights
#     print("\n📈 STATISTICAL INSIGHTS:")
#     print("-" * 60)
#     query_stats = """
#     SELECT 
#         species_name,
#         ROUND(MIN(petal_area), 2) as min_petal_area,
#         ROUND(MAX(petal_area), 2) as max_petal_area,
#         ROUND(AVG(petal_area), 2) as avg_petal_area,
#         ROUND(AVG(petal_sepal_ratio), 3) as avg_ratio
#     FROM iris_data
#     GROUP BY species_name
#     """
#     df_stats = pd.read_sql_query(query_stats, conn)
#     print(df_stats.to_string(index=False))
    
#     print("\n" + "="*60)
#     print("✓ Report generation completed!")
#     print("="*60 + "\n")
    
#     conn.close()


# # Define the DAG
# dag = DAG(
#     'iris_etl_pipeline',
#     default_args=default_args,
#     description='ETL pipeline for Iris dataset from sklearn',
#     schedule_interval='@daily',
#     catchup=False,
#     tags=['etl', 'sklearn', 'iris', 'ml'],
# )

# # Define tasks
# extract_task = PythonOperator(
#     task_id='extract_iris_dataset',
#     python_callable=extract_iris_data,
#     dag=dag,
# )

# transform_task = PythonOperator(
#     task_id='transform_iris_data',
#     python_callable=transform_iris_data,
#     dag=dag,
# )

# load_task = PythonOperator(
#     task_id='load_to_database',
#     python_callable=load_to_database,
#     dag=dag,
# )

# report_task = PythonOperator(
#     task_id='generate_analysis_report',
#     python_callable=generate_analysis_report,
#     dag=dag,
# )

# # Set task dependencies
# extract_task >> transform_task >> load_task >> report_task



# airflow-iris-etl\dags

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import os
import json
import pickle
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.metrics import precision_score, recall_score, f1_score
import numpy as np

# Default arguments for the DAG
default_args = {
    'owner': 'ml_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define paths
DATA_DIR = '/tmp/airflow_data'
RAW_DATA_PATH = f'{DATA_DIR}/iris_raw.csv'
PROCESSED_DATA_PATH = f'{DATA_DIR}/iris_processed.csv'
TRAIN_DATA_PATH = f'{DATA_DIR}/train_data.csv'
TEST_DATA_PATH = f'{DATA_DIR}/test_data.csv'
MODEL_DIR = f'{DATA_DIR}/models'
METRICS_PATH = f'{DATA_DIR}/model_metrics.json'
DB_PATH = f'{DATA_DIR}/iris.db'

# Model configuration
MODEL_THRESHOLD = 0.90  # Deploy if accuracy > 90%


def extract_iris_data():
    """Extract: Load Iris dataset from sklearn"""
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    # Load iris dataset
    iris = load_iris()
    
    # Create DataFrame
    df = pd.DataFrame(
        data=iris.data,
        columns=iris.feature_names
    )
    df['species'] = iris.target
    df['species_name'] = df['species'].map({
        0: 'setosa',
        1: 'versicolor', 
        2: 'virginica'
    })
    
    # Save raw data
    df.to_csv(RAW_DATA_PATH, index=False)
    print(f"✓ Iris dataset extracted: {len(df)} records")
    print(f"✓ Features: {list(iris.feature_names)}")
    print(f"✓ Species distribution:\n{df['species_name'].value_counts()}")


def feature_engineering():
    """Transform: Advanced feature engineering"""
    # Read raw data
    df = pd.read_csv(RAW_DATA_PATH)
    
    print("\n" + "="*60)
    print("FEATURE ENGINEERING")
    print("="*60)
    
    # 1. Geometric features
    df['petal_area'] = df['petal length (cm)'] * df['petal width (cm)']
    df['sepal_area'] = df['sepal length (cm)'] * df['sepal width (cm)']
    print("✓ Created area features")
    
    # 2. Ratios and proportions
    df['petal_sepal_length_ratio'] = df['petal length (cm)'] / df['sepal length (cm)']
    df['petal_sepal_width_ratio'] = df['petal width (cm)'] / df['sepal width (cm)']
    df['petal_sepal_area_ratio'] = df['petal_area'] / df['sepal_area']
    df['length_width_ratio'] = (df['petal length (cm)'] + df['sepal length (cm)']) / \
                                (df['petal width (cm)'] + df['sepal width (cm)'])
    print("✓ Created ratio features")
    
    # 3. Interaction features
    df['petal_length_x_width'] = df['petal length (cm)'] * df['petal width (cm)']
    df['sepal_length_x_width'] = df['sepal length (cm)'] * df['sepal width (cm)']
    print("✓ Created interaction features")
    
    # 4. Polynomial features
    df['petal_length_squared'] = df['petal length (cm)'] ** 2
    df['sepal_length_squared'] = df['sepal length (cm)'] ** 2
    print("✓ Created polynomial features")
    
    # 5. Statistical features
    df['total_length'] = df['petal length (cm)'] + df['sepal length (cm)']
    df['total_width'] = df['petal width (cm)'] + df['sepal width (cm)']
    df['total_area'] = df['petal_area'] + df['sepal_area']
    print("✓ Created statistical features")
    
    # 6. Size category (categorical encoding)
    df['size_category'] = pd.cut(
        df['petal length (cm)'],
        bins=[0, 2, 5, 7],
        labels=['Small', 'Medium', 'Large']
    )
    
    # 7. Quality score (composite metric)
    df['quality_score'] = (
        df['petal length (cm)'] * 0.3 +
        df['petal width (cm)'] * 0.2 +
        df['sepal length (cm)'] * 0.3 +
        df['sepal width (cm)'] * 0.2
    ).round(2)
    print("✓ Created composite features")
    
    # Save processed data
    df.to_csv(PROCESSED_DATA_PATH, index=False)
    
    print(f"\n✓ Feature engineering completed!")
    print(f"✓ Original features: 4")
    print(f"✓ Engineered features: {len(df.columns) - 6}")  # excluding original + target cols
    print(f"✓ Total features: {len(df.columns)}")
    print("="*60 + "\n")


def prepare_train_test_split():
    """Prepare training and test datasets"""
    df = pd.read_csv(PROCESSED_DATA_PATH)
    
    print("\n" + "="*60)
    print("TRAIN-TEST SPLIT")
    print("="*60)
    
    # Drop non-numeric and target columns for features
    feature_cols = [col for col in df.columns if col not in 
                   ['species', 'species_name', 'size_category']]
    
    X = df[feature_cols]
    y = df['species']
    
    # Split data (80-20)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    # Save splits
    train_df = X_train.copy()
    train_df['species'] = y_train
    train_df.to_csv(TRAIN_DATA_PATH, index=False)
    
    test_df = X_test.copy()
    test_df['species'] = y_test
    test_df.to_csv(TEST_DATA_PATH, index=False)
    
    print(f"✓ Training set: {len(train_df)} samples")
    print(f"✓ Test set: {len(test_df)} samples")
    print(f"✓ Features used: {len(feature_cols)}")
    print(f"✓ Class distribution (train):\n{y_train.value_counts()}")
    print("="*60 + "\n")


def train_models():
    """Train multiple ML models with hyperparameter tuning"""
    print("\n" + "="*60)
    print("MODEL TRAINING")
    print("="*60)
    
    # Load data
    train_df = pd.read_csv(TRAIN_DATA_PATH)
    X_train = train_df.drop('species', axis=1)
    y_train = train_df['species']
    
    # Standardize features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    
    # Save scaler
    with open(f'{MODEL_DIR}/scaler.pkl', 'wb') as f:
        pickle.dump(scaler, f)
    print("✓ Feature scaling completed and scaler saved")
    
    models = {
        'random_forest': {
            'model': RandomForestClassifier(random_state=42),
            'params': {
                'n_estimators': [50, 100, 200],
                'max_depth': [5, 10, None],
                'min_samples_split': [2, 5]
            }
        },
        'gradient_boosting': {
            'model': GradientBoostingClassifier(random_state=42),
            'params': {
                'n_estimators': [50, 100],
                'learning_rate': [0.01, 0.1, 0.2],
                'max_depth': [3, 5]
            }
        },
        'logistic_regression': {
            'model': LogisticRegression(random_state=42, max_iter=1000),
            'params': {
                'C': [0.1, 1.0, 10.0],
                'penalty': ['l2']
            }
        }
    }
    
    best_models = {}
    
    for name, config in models.items():
        print(f"\n🔧 Training {name}...")
        
        # Grid search with cross-validation
        grid_search = GridSearchCV(
            config['model'],
            config['params'],
            cv=5,
            scoring='accuracy',
            n_jobs=-1
        )
        
        grid_search.fit(X_train_scaled, y_train)
        
        # Save best model
        best_model = grid_search.best_estimator_
        model_path = f'{MODEL_DIR}/{name}.pkl'
        with open(model_path, 'wb') as f:
            pickle.dump(best_model, f)
        
        best_models[name] = {
            'model': best_model,
            'score': grid_search.best_score_,
            'params': grid_search.best_params_
        }
        
        print(f"  ✓ Best CV Score: {grid_search.best_score_:.4f}")
        print(f"  ✓ Best Params: {grid_search.best_params_}")
        print(f"  ✓ Model saved: {model_path}")
    
    print("\n" + "="*60)
    print("✓ All models trained successfully!")
    print("="*60 + "\n")


def evaluate_models():
    """Evaluate all trained models on test set"""
    print("\n" + "="*60)
    print("MODEL EVALUATION")
    print("="*60)
    
    # Load test data
    test_df = pd.read_csv(TEST_DATA_PATH)
    X_test = test_df.drop('species', axis=1)
    y_test = test_df['species']
    
    # Load scaler
    with open(f'{MODEL_DIR}/scaler.pkl', 'rb') as f:
        scaler = pickle.load(f)
    X_test_scaled = scaler.transform(X_test)
    
    # Evaluate each model
    model_results = {}
    
    for model_name in ['random_forest', 'gradient_boosting', 'logistic_regression']:
        print(f"\n📊 Evaluating {model_name}...")
        
        # Load model
        with open(f'{MODEL_DIR}/{model_name}.pkl', 'rb') as f:
            model = pickle.load(f)
        
        # Predictions
        y_pred = model.predict(X_test_scaled)
        
        # Calculate metrics
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, average='weighted')
        recall = recall_score(y_test, y_pred, average='weighted')
        f1 = f1_score(y_test, y_pred, average='weighted')
        
        model_results[model_name] = {
            'accuracy': float(accuracy),
            'precision': float(precision),
            'recall': float(recall),
            'f1_score': float(f1),
            'confusion_matrix': confusion_matrix(y_test, y_pred).tolist()
        }
        
        print(f"  Accuracy:  {accuracy:.4f}")
        print(f"  Precision: {precision:.4f}")
        print(f"  Recall:    {recall:.4f}")
        print(f"  F1-Score:  {f1:.4f}")
        print(f"\n  Classification Report:\n{classification_report(y_test, y_pred)}")
    
    # Find best model
    best_model_name = max(model_results, key=lambda x: model_results[x]['accuracy'])
    best_accuracy = model_results[best_model_name]['accuracy']
    
    model_results['best_model'] = best_model_name
    model_results['best_accuracy'] = best_accuracy
    model_results['evaluation_date'] = datetime.now().isoformat()
    
    # Save metrics
    with open(METRICS_PATH, 'w') as f:
        json.dump(model_results, f, indent=2)
    
    print("\n" + "="*60)
    print(f"🏆 BEST MODEL: {best_model_name}")
    print(f"🎯 Accuracy: {best_accuracy:.4f}")
    print("="*60 + "\n")


def check_model_performance():
    """Branch based on model performance"""
    with open(METRICS_PATH, 'r') as f:
        metrics = json.load(f)
    
    best_accuracy = metrics['best_accuracy']
    
    print(f"\n🎯 Best Model Accuracy: {best_accuracy:.4f}")
    print(f"📊 Threshold: {MODEL_THRESHOLD}")
    
    if best_accuracy >= MODEL_THRESHOLD:
        print("✅ Model meets deployment criteria!")
        return 'deploy_model'
    else:
        print("❌ Model does not meet deployment criteria")
        return 'retrain_notification'


def deploy_model():
    """Deploy best model to production"""
    print("\n" + "="*60)
    print("MODEL DEPLOYMENT")
    print("="*60)
    
    with open(METRICS_PATH, 'r') as f:
        metrics = json.load(f)
    
    best_model_name = metrics['best_model']
    
    # Copy best model to production path
    import shutil
    src = f'{MODEL_DIR}/{best_model_name}.pkl'
    dst = f'{MODEL_DIR}/production_model.pkl'
    shutil.copy(src, dst)
    
    # Copy scaler
    shutil.copy(f'{MODEL_DIR}/scaler.pkl', f'{MODEL_DIR}/production_scaler.pkl')
    
    print(f"✅ Deployed model: {best_model_name}")
    print(f"✅ Model path: {dst}")
    print(f"✅ Accuracy: {metrics['best_accuracy']:.4f}")
    print("="*60 + "\n")


def retrain_notification():
    """Send notification for retraining"""
    print("\n" + "="*60)
    print("RETRAIN NOTIFICATION")
    print("="*60)
    print("⚠️  Model performance below threshold")
    print("📧 Notification sent to ML team")
    print("🔄 Consider feature engineering or hyperparameter tuning")
    print("="*60 + "\n")


def load_to_database():
    """Load all data and metrics to database"""
    conn = sqlite3.connect(DB_PATH)
    
    # Load processed data
    df = pd.read_csv(PROCESSED_DATA_PATH)
    df.to_sql('iris_data', conn, if_exists='replace', index=False)
    
    # Load model metrics
    with open(METRICS_PATH, 'r') as f:
        metrics = json.load(f)
    
    # Create metrics table
    metrics_df = pd.DataFrame([{
        'model_name': k,
        'accuracy': v.get('accuracy', 0),
        'precision': v.get('precision', 0),
        'recall': v.get('recall', 0),
        'f1_score': v.get('f1_score', 0),
        'is_best': (k == metrics['best_model'])
    } for k, v in metrics.items() if k not in ['best_model', 'best_accuracy', 'evaluation_date']])
    
    metrics_df.to_sql('model_metrics', conn, if_exists='replace', index=False)
    
    print("✓ Data loaded to database")
    print(f"✓ Tables: iris_data, model_metrics")
    
    conn.close()


def generate_ml_report():
    """Generate comprehensive ML pipeline report"""
    conn = sqlite3.connect(DB_PATH)
    
    print("\n" + "="*70)
    print("ML PIPELINE REPORT")
    print("="*70)
    
    # Model Performance
    print("\n📊 MODEL PERFORMANCE COMPARISON:")
    print("-" * 70)
    query = """
    SELECT model_name, 
           ROUND(accuracy, 4) as accuracy,
           ROUND(precision, 4) as precision,
           ROUND(recall, 4) as recall,
           ROUND(f1_score, 4) as f1_score,
           CASE WHEN is_best = 1 THEN '🏆' ELSE '' END as best
    FROM model_metrics
    ORDER BY accuracy DESC
    """
    df_metrics = pd.read_sql_query(query, conn)
    print(df_metrics.to_string(index=False))
    
    # Feature importance (if available)
    with open(METRICS_PATH, 'r') as f:
        metrics = json.load(f)
    
    print(f"\n🎯 BEST MODEL: {metrics['best_model']}")
    print(f"📈 Accuracy: {metrics['best_accuracy']:.4f}")
    print(f"📅 Evaluation Date: {metrics['evaluation_date']}")
    
    # Data Summary
    print("\n📋 DATASET SUMMARY:")
    print("-" * 70)
    query = """
    SELECT 
        COUNT(*) as total_samples,
        COUNT(DISTINCT species) as num_classes,
        ROUND(AVG(quality_score), 2) as avg_quality_score
    FROM iris_data
    """
    df_summary = pd.read_sql_query(query, conn)
    print(df_summary.to_string(index=False))
    
    print("\n" + "="*70)
    print("✅ ML Pipeline Execution Completed!")
    print("="*70 + "\n")
    
    conn.close()


# Define the DAG
dag = DAG(
    'iris_ml_pipeline',
    default_args=default_args,
    description='End-to-end ML pipeline with training and deployment',
    schedule_interval='@daily',
    catchup=False,
    tags=['ml', 'sklearn', 'iris', 'training', 'deployment'],
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_iris_dataset',
    python_callable=extract_iris_data,
    dag=dag,
)

feature_eng_task = PythonOperator(
    task_id='feature_engineering',
    python_callable=feature_engineering,
    dag=dag,
)

split_task = PythonOperator(
    task_id='train_test_split',
    python_callable=prepare_train_test_split,
    dag=dag,
)

train_task = PythonOperator(
    task_id='train_models',
    python_callable=train_models,
    dag=dag,
)

evaluate_task = PythonOperator(
    task_id='evaluate_models',
    python_callable=evaluate_models,
    dag=dag,
)

check_performance_task = BranchPythonOperator(
    task_id='check_model_performance',
    python_callable=check_model_performance,
    dag=dag,
)

deploy_task = PythonOperator(
    task_id='deploy_model',
    python_callable=deploy_model,
    dag=dag,
)

retrain_task = PythonOperator(
    task_id='retrain_notification',
    python_callable=retrain_notification,
    dag=dag,
)

load_db_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    trigger_rule='none_failed',
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_ml_report',
    python_callable=generate_ml_report,
    trigger_rule='none_failed',
    dag=dag,
)

# Set task dependencies
extract_task >> feature_eng_task >> split_task >> train_task >> evaluate_task
evaluate_task >> check_performance_task >> [deploy_task, retrain_task]
[deploy_task, retrain_task] >> load_db_task >> report_task