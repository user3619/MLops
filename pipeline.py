import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import os
import json

def pipeline():
    print("="*50)
    print("Starting ML Pipeline...")
    print("="*50)

    data_path = '/home/stepan/airflow/data/raw/diamonds.csv'

    if not os.path.exists(data_path):
        print(f"File not found at {data_path}. Generating synthetic data.")
        from sklearn.datasets import make_regression
        X, y = make_regression(n_samples=1000, n_features=5, noise=0.1, random_state=42)
        df = pd.DataFrame(X, columns=['carat', 'depth', 'table', 'x', 'y'])
        df['price'] = y
        os.makedirs(os.path.dirname(data_path), exist_ok=True)
        df.to_csv(data_path, index=False)
        print("Synthetic data generated and saved.")

    df = pd.read_csv(data_path)
    print(f"Data loaded. Shape: {df.shape}")

    numeric_features = df.select_dtypes(include=['int64', 'float64']).columns.drop('price')
    categorical_features = df.select_dtypes(include=['object']).columns

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', 'passthrough', numeric_features),
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
        ])

    X = df.drop('price', axis=1)
    y = df['price']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(f"Data split. Train size: {X_train.shape[0]}, Test size: {X_test.shape[0]}")

    model = RandomForestRegressor(n_estimators=10, max_depth=10, random_state=42)
    pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                               ('regressor', model)])
    pipeline.fit(X_train, y_train)
    print("Model training completed.")

    y_pred = pipeline.predict(X_test)
    metrics = {
        'mae': mean_absolute_error(y_test, y_pred),
        'mse': mean_squared_error(y_test, y_pred),
        'r2': r2_score(y_test, y_pred)
    }
    print(f"Model validation metrics: {metrics}")


    model_path = '/home/stepan/airflow/models/model.pkl'
    metrics_path = '/home/stepan/airflow/models/metrics.json'
    joblib.dump(pipeline, model_path)
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f)
    print(f"Model and metrics saved to {model_path}")

    return "Pipeline finished successfully"

if __name__ == "__main__":
    pipeline()
