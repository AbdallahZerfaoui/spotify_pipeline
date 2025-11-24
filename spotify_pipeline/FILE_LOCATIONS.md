# 📂 File Locations Guide

## Project Structure

```
/Users/zerfaouiabdallah/Spotify/
│
├── data/                                    # DATA FOLDER (at project root)
│   ├── spotify_data.csv                    # Original dataset
│   │
│   ├── dagster_output/                     # Human-readable pipeline outputs
│   │   ├── 01_raw_data.csv                # Raw loaded data
│   │   ├── 02_cleaned_data.csv            # After cleaning
│   │   ├── 03_features.csv                # Feature matrix
│   │   ├── 03_target.csv                  # Target labels
│   │   ├── 03_feature_names.txt           # List of feature names
│   │   ├── 04_X_train.csv                 # Training features
│   │   ├── 04_X_test.csv                  # Test features
│   │   ├── 04_y_train.csv                 # Training labels
│   │   └── 04_y_test.csv                  # Test labels
│   │
│   └── dagster_storage/                    # Dagster internal storage (pickles)
│       ├── raw_spotify_csv/
│       ├── spotify_cleaned/
│       ├── spotify_features/
│       ├── train_test_split/
│       ├── xgboost_model/
│       └── model_predictions/
│
├── models/                                  # MODELS FOLDER (at project root)
│   ├── xgboost_model.pkl                   # ✅ Trained model (2.2 MB)
│   ├── model_metadata.json                 # ✅ Model performance info
│   └── test_predictions.csv                # ✅ Predictions with probabilities
│
└── spotify_pipeline/                        # Pipeline code directory
    ├── api.py                              # FastAPI application
    ├── test_api.py                         # API test script
    ├── predict_from_csv.py                 # CSV prediction utility
    ├── start.sh                            # Start Dagster UI
    ├── start_api.sh                        # Start API server
    │
    └── src/spotify_pipeline/
        ├── definitions.py                  # Pipeline entry point
        │
        ├── assets/                         # Assets folder (used by pipeline)
        │   ├── __init__.py
        │   ├── data_assets.py             # Data pipeline assets
        │   ├── model_assets.py            # Model training assets
        │   └── resources.py
        │
        └── defs/                           # Alternative defs folder
            ├── __init__.py
            ├── data_assets.py
            ├── model_assets.py
            └── resources.py
```

## 🎯 Key Locations

### Where Models Are Saved
**Location:** `/Users/zerfaouiabdallah/Spotify/models/`

This is at the **project root** (Spotify folder), NOT inside spotify_pipeline!

Files in this folder:
- `xgboost_model.pkl` - The trained XGBoost classifier
- `model_metadata.json` - Model performance metrics
- `test_predictions.csv` - Predictions on test set

### Where Data Outputs Are Saved
**Location:** `/Users/zerfaouiabdallah/Spotify/data/dagster_output/`

Human-readable CSV files with numbered prefixes (01-04) for each pipeline stage.

### Where Dagster Stores Internal Data
**Location:** `/Users/zerfaouiabdallah/Spotify/data/dagster_storage/`

These are pickle files used internally by Dagster for caching and asset management.

## 🔍 How Path Resolution Works

Both `data_assets.py` and `model_assets.py` use the `_find_project_root()` helper function:

```python
def _find_project_root() -> Path:
    """Find the Spotify project root directory."""
    current = Path.cwd()
    
    # Try to find Spotify folder
    for parent in [current, current.parent, current.parent.parent]:
        if parent.name == "Spotify" or (parent / "data").exists():
            return parent
    
    return current.parent if current.name == "spotify_pipeline" else current
```

This ensures files are saved to the correct location regardless of where you run the commands from:
- Running from `spotify_pipeline/` → finds parent `Spotify/`
- Running from `Spotify/` → finds current directory
- Running from nested folders → searches up the tree

## 🚀 Where Commands Should Be Run

### Dagster Pipeline
```bash
cd /Users/zerfaouiabdallah/Spotify/spotify_pipeline
./start.sh
```
- Runs from: `spotify_pipeline/`
- Saves models to: `../models/` (project root)
- Saves data to: `../data/dagster_output/`

### FastAPI Server
```bash
cd /Users/zerfaouiabdallah/Spotify/spotify_pipeline
./start_api.sh
```
- Runs from: `spotify_pipeline/`
- Loads model from: `../models/xgboost_model.pkl`
- Loads features from: `../data/dagster_output/03_feature_names.txt`

### API Tests
```bash
cd /Users/zerfaouiabdallah/Spotify/spotify_pipeline
uv run python test_api.py
# OR
./test_api_curl.sh
# OR
uv run python predict_from_csv.py
```

## ✅ Verification Commands

Check if model exists:
```bash
ls -lh /Users/zerfaouiabdallah/Spotify/models/
```

Check if data outputs exist:
```bash
ls -lh /Users/zerfaouiabdallah/Spotify/data/dagster_output/
```

View model metadata:
```bash
cat /Users/zerfaouiabdallah/Spotify/models/model_metadata.json | python3 -m json.tool
```

Check model size:
```bash
du -h /Users/zerfaouiabdallah/Spotify/models/xgboost_model.pkl
```

## 🐛 Troubleshooting

### "Model not found" when starting API
**Problem:** API can't find `xgboost_model.pkl`

**Check:**
```bash
ls /Users/zerfaouiabdallah/Spotify/models/xgboost_model.pkl
```

**Solution:** Run the Dagster pipeline to train and save the model

### Model saving to wrong location
**Problem:** Model saved to `spotify_pipeline/models/` instead of `Spotify/models/`

**Solution:** Make sure you're using the updated `model_assets.py` files that use `_find_project_root()`

**Move existing file:**
```bash
mv /Users/zerfaouiabdallah/Spotify/spotify_pipeline/models/xgboost_model.pkl \
   /Users/zerfaouiabdallah/Spotify/models/
```

### Can't find feature names
**Problem:** `03_feature_names.txt` not found

**Solution:** Run the complete pipeline to generate all data outputs

**Check:**
```bash
ls /Users/zerfaouiabdallah/Spotify/data/dagster_output/
```

## 📝 Summary

- **Models** → `/Users/zerfaouiabdallah/Spotify/models/`
- **Data** → `/Users/zerfaouiabdallah/Spotify/data/`
- **Code** → `/Users/zerfaouiabdallah/Spotify/spotify_pipeline/`

All paths are resolved relative to the **project root** (`Spotify/` folder), ensuring consistency regardless of execution context.
