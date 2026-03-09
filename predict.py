import pandas as pd
import joblib
import os
import json

def predict():
    print("="*50)
    print("Starting Prediction...")
    print("="*50)

    model_path = '/home/stepan/airflow/models/model.pkl'
    test_data_path = '/home/stepan/airflow/data/test/diamonds_new.csv'
    predictions_dir = '/home/stepan/airflow/predictions/'
    predictions_file = os.path.join(predictions_dir, 'predictions.csv')

    if not os.path.exists(model_path):
        print(f"Error: Model not found at {model_path}")
        return "Prediction failed: Model not found"

    print("Loading model...")
    pipeline = joblib.load(model_path)
    print("Model loaded successfully.")

    expected_columns = ['carat', 'cut', 'color', 'clarity', 'depth', 'table', 'x', 'y', 'z']
    
    if not os.path.exists(test_data_path):
        print(f"Test data not found at {test_data_path}. Generating sample data.")
        os.makedirs(os.path.dirname(test_data_path), exist_ok=True)

        sample_data = {
            'carat': [0.3, 0.5, 1.0, 1.5, 2.0],
            'cut': ['Ideal', 'Premium', 'Very Good', 'Good', 'Fair'],
            'color': ['D', 'E', 'F', 'G', 'H'],
            'clarity': ['IF', 'VVS1', 'VVS2', 'VS1', 'VS2'],
            'depth': [61.5, 62.0, 61.8, 62.5, 63.0],
            'table': [55.0, 57.0, 59.0, 60.0, 61.0],
            'x': [4.3, 5.1, 6.5, 7.0, 7.5],
            'y': [4.3, 5.1, 6.5, 7.0, 7.5],
            'z': [2.8, 3.2, 4.0, 4.4, 4.7]
        }
        
        df_test = pd.DataFrame(sample_data)
        df_test.to_csv(test_data_path, index=False)
        print(f"Sample test data created. Columns: {list(df_test.columns)}")
    else:
        df_test = pd.read_csv(test_data_path)
        print(f"Test data loaded. Shape: {df_test.shape}")
        print(f"Test data columns: {list(df_test.columns)}")

    missing_columns = set(expected_columns) - set(df_test.columns)
    if missing_columns:
        print(f"ERROR: Missing columns in test data: {missing_columns}")
        return f"Prediction failed: Missing columns {missing_columns}"
    
    df_test = df_test[expected_columns]

    print("Making predictions...")
    predictions = pipeline.predict(df_test)
    df_test['predicted_price'] = predictions
    print(f"Predictions made: {predictions}")

    os.makedirs(predictions_dir, exist_ok=True)
    df_test.to_csv(predictions_file, index=False)
    print(f"Predictions saved to {predictions_file}")

    metrics = {
        'num_predictions': len(predictions),
        'min_prediction': float(predictions.min()),
        'max_prediction': float(predictions.max()),
        'mean_prediction': float(predictions.mean())
    }
    
    metrics_path = os.path.join(predictions_dir, 'prediction_metrics.json')
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f)
    print(f"Prediction metrics saved: {metrics}")

    return "Prediction finished successfully"

if __name__ == "__main__":
    predict()
