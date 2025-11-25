"""Model training and evaluation assets."""

from pathlib import Path
from typing import Any, Dict
import pickle
import json

import pandas as pd
from dagster import AssetIn, Output, asset
from sklearn.metrics import classification_report, accuracy_score
from xgboost import XGBClassifier

SEED = 42


def _find_project_root() -> Path:
    """Find the Spotify project root directory."""
    current = Path.cwd()
    
    # Try to find Spotify folder
    for parent in [current, current.parent, current.parent.parent]:
        if parent.name == "Spotify" or (parent / "data").exists():
            return parent
    
    return current.parent if current.name == "spotify_pipeline" else current


@asset(
    ins={"split_data": AssetIn("train_test_split")},
    description="Train XGBoost model on Spotify data",
)
def xgboost_model(context, split_data: Dict[str, Any]) -> Output[Dict[str, Any]]:
    """Train an XGBoost classifier and save to disk."""
    X_train = split_data["X_train"]
    y_train = split_data["y_train"]
    X_test = split_data["X_test"]
    y_test = split_data["y_test"]
    
    context.log.info(f"Training XGBoost with {X_train.shape[0]} samples, {X_train.shape[1]} features")
    
    # Train model with parameters from notebook
    model = XGBClassifier(
        random_state=SEED,
        objective='binary:logistic',
        colsample_bytree=0.8,
        learning_rate=0.1,
        max_depth=6,
        n_estimators=1000,
        subsample=0.9,
        scale_pos_weight=6,
        n_jobs=-1
    )
    
    model.fit(X_train, y_train)
    
    # Evaluate
    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)
    report = classification_report(y_test, predictions, output_dict=True)
    
    # Save model to models directory at project root
    project_root = _find_project_root()
    models_dir = project_root / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    
    model_path = models_dir / "xgboost_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    
    context.log.info(f"✅ Model saved to: {model_path}")
    
    # Save human-readable metadata as JSON
    metadata_dict = {
        "model_type": "XGBoost Classifier",
        "hyperparameters": {
            "n_estimators": 1000,
            "max_depth": 6,
            "learning_rate": 0.1,
            "colsample_bytree": 0.8,
            "subsample": 0.9,
            "scale_pos_weight": 6,
            "random_state": SEED,
        },
        "performance": {
            "accuracy": float(accuracy),
            "class_0": {
                "precision": float(report["0"]["precision"]),
                "recall": float(report["0"]["recall"]),
                "f1-score": float(report["0"]["f1-score"]),
            },
            "class_1": {
                "precision": float(report["1"]["precision"]),
                "recall": float(report["1"]["recall"]),
                "f1-score": float(report["1"]["f1-score"]),
            },
        },
        "training_samples": int(X_train.shape[0]),
        "test_samples": int(X_test.shape[0]),
        "num_features": int(X_train.shape[1]),
    }
    
    metadata_path = models_dir / "model_metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata_dict, f, indent=2)
    
    context.log.info(f"✅ Metadata saved to: {metadata_path}")
    context.log.info(f"Accuracy: {accuracy:.4f}")
    
    return Output(
        {
            "model": model,
            "model_path": str(model_path),
            "accuracy": accuracy,
            "classification_report": report,
        },
        metadata={
            "accuracy": accuracy,
            "recall_class_1": report["1"]["recall"],
            "precision_class_1": report["1"]["precision"],
            "model_path": str(model_path),
            "metadata_path": str(metadata_path),
        },
    )


@asset(
    ins={"model_info": AssetIn("xgboost_model"), "split_data": AssetIn("train_test_split")},
    required_resource_keys={"file_config"},
    description="Generate predictions on test data",
)
def model_predictions(context, model_info: Dict[str, Any], split_data: Dict[str, Any]) -> Output[pd.DataFrame]:
    """Generate predictions on test set and save results."""
    model = model_info["model"]
    X_test = split_data["X_test"]
    y_test = split_data["y_test"]
    
    # Get predictions and probabilities
    predictions = model.predict(X_test)
    probabilities = model.predict_proba(X_test)
    
    # Create DataFrame with results
    predictions_df = pd.DataFrame({
        'actual': y_test.values,
        'predicted': predictions,
        'probability_class_0': probabilities[:, 0],
        'probability_class_1': probabilities[:, 1],
        'correct': (y_test.values == predictions).astype(int)
    })
    
    # Calculate class-specific accuracy
    class_0_mask = predictions_df['actual'] == 0
    class_1_mask = predictions_df['actual'] == 1
    
    class_0_correct = predictions_df[class_0_mask]['correct'].sum()
    class_0_total = class_0_mask.sum()
    class_1_correct = predictions_df[class_1_mask]['correct'].sum()
    class_1_total = class_1_mask.sum()
    
    context.log.info(f"Total predictions: {len(predictions_df)}")
    context.log.info(f"Overall accuracy: {predictions_df['correct'].mean():.4f}")
    context.log.info(f"Class 0 accuracy: {class_0_correct}/{class_0_total} ({100*class_0_correct/class_0_total:.2f}%)")
    context.log.info(f"Class 1 accuracy: {class_1_correct}/{class_1_total} ({100*class_1_correct/class_1_total:.2f}%)")
    
    # Save predictions to CSV
    project_root = _find_project_root()
    models_dir = project_root / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    
    predictions_path = models_dir / "test_predictions.csv"
    predictions_df.to_csv(predictions_path, index=False)
    context.log.info(f"✅ Predictions saved to: {predictions_path.absolute()}")
    
    return Output(
        predictions_df,
        metadata={
            "num_predictions": len(predictions_df),
            "overall_accuracy": float(predictions_df['correct'].mean()),
            "class_0_accuracy": float(class_0_correct / class_0_total),
            "class_1_accuracy": float(class_1_correct / class_1_total),
            "predictions_path": str(predictions_path),
        },
    )
