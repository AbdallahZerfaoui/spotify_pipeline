"""Assets responsible for:
- Loading the raw Kaggle CSV
- Cleaning / preprocessing
- Feature engineering
- Train/test splitting
"""

from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass

import pandas as pd
from dagster import asset, Output, AssetIn
from sklearn.model_selection import train_test_split as sk_train_test_split


POPULARITY_THRESHOLD = 85
SEED = 42


@dataclass
class FeatureSet:
    """Container for features and target."""
    features: pd.DataFrame
    target: pd.Series


# -----------------------------------------------------------
# 1. Load raw data
# -----------------------------------------------------------

def _find_project_root() -> Path:
    """Find the Spotify project root directory."""
    current = Path.cwd()
    
    # Try to find Spotify folder
    for parent in [current, current.parent, current.parent.parent]:
        if parent.name == "Spotify" or (parent / "data").exists():
            return parent
    
    return current.parent if current.name == "spotify_pipeline" else current



@asset(
    required_resource_keys={"file_config"},
    description="Load the raw Spotify CSV from data folder"
)
def raw_spotify_csv(context) -> Output[pd.DataFrame]:
    """Load and combine all Spotify CSV files from data/ folder."""
    import os
    
    project_root = _find_project_root()
    data_dir = project_root / "data"
    
    context.log.info(f"Project root: {project_root}")
    context.log.info(f"Looking for CSV in: {data_dir}")
    
    if not data_dir.exists():
        raise FileNotFoundError(
            f"Data directory not found at: {data_dir}\n"
            f"Project root detected as: {project_root}"
        )
    
    # Find CSV files (excluding dagster_output)
    csv_files = [f for f in data_dir.glob("*.csv") if "dagster_output" not in str(f)]
    
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV file found in {data_dir}. "
            "Please ensure spotify_data.csv is in the data/ folder."
        )
    
    # ✅ FIX: Load and combine ALL CSV files with proper header handling
    context.log.info(f"Found {len(csv_files)} CSV file(s): {sorted([f.name for f in csv_files])}")
    
    dfs = []
    for csv_path in sorted(csv_files):
        try:
            context.log.info(f"📂 Loading: {csv_path.name}")
            
            # Load CSV - explicitly set header row to 0
            df_temp = pd.read_csv(csv_path, header=0)
            
            context.log.info(f"  ✅ Loaded {len(df_temp)} rows × {len(df_temp.columns)} columns")
            context.log.info(f"  Columns: {list(df_temp.columns)[:10]}...")  # Show first 10 columns
            
            # Check for malformed data
            if len(df_temp.columns) > 100:
                context.log.warning(f"  ⚠️  WARNING: {len(df_temp.columns)} columns detected!")
                context.log.warning(f"     This might indicate the CSV has data as column names")
                context.log.warning(f"     First few column names: {list(df_temp.columns)[:5]}")
            
            dfs.append(df_temp)
            
        except Exception as e:
            context.log.error(f"❌ ERROR loading {csv_path.name}: {str(e)}")
            raise
    
    if not dfs:
        raise ValueError("No valid dataframes loaded!")
    
    # Check if all dataframes have the same columns
    first_columns = set(dfs[0].columns)
    for i, df_temp in enumerate(dfs[1:], 1):
        temp_columns = set(df_temp.columns)
        if temp_columns != first_columns:
            context.log.warning(f"⚠️  CSV {i} has different columns!")
            context.log.warning(f"   Missing from this file: {first_columns - temp_columns}")
            context.log.warning(f"   Extra in this file: {temp_columns - first_columns}")
    
    # Combine all dataframes
    df = pd.concat(dfs, ignore_index=True)
    context.log.info(f"🔀 Combined {len(dfs)} files:")
    context.log.info(f"   Total: {len(df)} rows × {len(df.columns)} columns")
    
    # Check for duplicate column names
    if df.columns.duplicated().any():
        context.log.warning(f"⚠️  Duplicate column names detected!")
        context.log.warning(f"   Duplicates: {df.columns[df.columns.duplicated()].tolist()}")
    
    # Basic data quality checks
    context.log.info(f"📊 Data Quality:")
    context.log.info(f"   Total null values: {df.isna().sum().sum()}")
    context.log.info(f"   Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Save a readable copy for inspection
    output_dir = data_dir / "dagster_output"
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / "01_raw_data.csv"
    df.to_csv(output_path, index=False)
    context.log.info(f"✅ Saved readable copy to: {output_path}")
    
    return Output(
        df,
        metadata={
            "rows": len(df),
            "columns": len(df.columns),
            "num_files": len(csv_files),
            "files_combined": sorted([f.name for f in csv_files]),
            "readable_copy": str(output_path),
        },
    )


# -----------------------------------------------------------
# 2. Clean data
# -----------------------------------------------------------

@asset(
    ins={"df": AssetIn("raw_spotify_csv")},
    description="Clean raw dataset: missing values, types, filters."
)
def spotify_cleaned(context, df: pd.DataFrame) -> Output[pd.DataFrame]:
    """Clean data: remove nulls, filter zeros, create verdict column."""
    context.log.info(f"🔍 DIAGNOSTIC START")
    context.log.info(f"Input shape: {df.shape}")
    context.log.info(f"Input columns: {list(df.columns)}")
    
    # Drop rows with missing values
    df_before_null = len(df)
    df = df.dropna()
    df_after_null = len(df)
    context.log.info(f"After dropna(): {df_before_null} → {df_after_null} rows (removed {df_before_null - df_after_null})")
    
    if len(df) == 0:
        context.log.error("❌ ERROR: All rows removed by dropna()!")
        raise ValueError("All data removed after dropping NaN values")
    
    # Filter out tracks with zero popularity
    df_before_pop = len(df)
    df = df[df['popularity'] > 0]
    df_after_pop = len(df)
    context.log.info(f"After popularity > 0: {df_before_pop} → {df_after_pop} rows (removed {df_before_pop - df_after_pop})")
    
    if len(df) == 0:
        context.log.error("❌ ERROR: All rows removed by popularity filter!")
        raise ValueError("All data removed after filtering popularity > 0")
    
    # Ensure year column exists
    context.log.info(f"Checking for 'year' column...")
    if 'year' not in df.columns:
        context.log.info(f"'year' not found, checking for 'release_date'...")
        if 'release_date' in df.columns:
            context.log.info(f"Converting 'release_date' to 'year'...")
            df['year'] = pd.to_datetime(df['release_date'], errors='coerce').dt.year
            df_before_year = len(df)
            df = df.dropna(subset=['year'])
            df_after_year = len(df)
            context.log.info(f"After dropna(subset=['year']): {df_before_year} → {df_after_year} rows (removed {df_before_year - df_after_year})")
        else:
            context.log.error("❌ ERROR: Neither 'year' nor 'release_date' column found!")
            context.log.info(f"Available columns: {list(df.columns)}")
            raise ValueError("'year' or 'release_date' column required")
    
    if len(df) == 0:
        context.log.error("❌ ERROR: All rows removed by year processing!")
        raise ValueError("All data removed after year processing")
    
    df['year'] = df['year'].astype(int)
    context.log.info(f"✅ Year column ready")
    
    # Add artist song count feature
    if 'artist_name' in df.columns and 'track_id' in df.columns:
        context.log.info(f"Adding 'artist_song_count' feature...")
        df['artist_song_count'] = df.groupby('artist_name')['track_id'].transform('count')
        context.log.info(f"✅ artist_song_count added")
    else:
        context.log.warning(f"⚠️  Cannot add artist_song_count: missing artist_name or track_id")
    
    # Create verdict column (binary target: popular vs not popular)
    context.log.info(f"Creating 'verdict' column with popularity threshold {POPULARITY_THRESHOLD}%...")
    yearly_thresholds = df.groupby('year')['popularity'].quantile(
        POPULARITY_THRESHOLD / 100
    ).to_dict()
    context.log.info(f"Yearly thresholds: {yearly_thresholds}")
    
    df['verdict'] = df.apply(
        lambda row: 1 if row['popularity'] >= yearly_thresholds.get(row['year'], POPULARITY_THRESHOLD) else 0,
        axis=1
    )
    context.log.info(f"✅ verdict column created")
    
    # Add duration-based features
    if 'duration_ms' in df.columns:
        context.log.info(f"Adding duration-based features...")
        q1 = df['duration_ms'].quantile(0.25)
        q95 = df['duration_ms'].quantile(0.95)
        df['long_duration'] = (df['duration_ms'] > q95).astype(int)
        df['short_duration'] = (df['duration_ms'] < q1).astype(int)
        context.log.info(f"✅ Duration features added")
    else:
        context.log.warning(f"⚠️  'duration_ms' column not found")
    
    positive_pct = df['verdict'].mean() * 100
    context.log.info(f"✅ Final shape: {df.shape}")
    context.log.info(f"Positive class (popular): {positive_pct:.2f}%")
    context.log.info(f"Verdict value counts:\n{df['verdict'].value_counts()}")
    
    # Save readable copy
    project_root = _find_project_root()
    output_dir = project_root / "data" / "dagster_output"
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / "02_cleaned_data.csv"
    df.to_csv(output_path, index=False)
    context.log.info(f"✅ Saved readable copy to: {output_path}")
    
    return Output(
        df,
        metadata={
            "rows": len(df),
            "columns": len(df.columns),
            "positive_class_pct": float(positive_pct),
            "readable_copy": str(output_path),
        },
    )

# -----------------------------------------------------------
# 3. Feature engineering
# -----------------------------------------------------------

@asset(
    ins={"df": AssetIn("spotify_cleaned")},
    description="Feature engineering: encoders, scalers, transformations."
)
def spotify_features(context, df: pd.DataFrame) -> Output[FeatureSet]:
    """Prepare features for modeling: one-hot encode genre, drop unnecessary columns."""
    
    context.log.info(f"🔍 DIAGNOSTIC START: Input shape: {df.shape}")
    context.log.info(f"Columns in input: {list(df.columns)}")
    context.log.info(f"Rows in input: {len(df)}")
    
    if len(df) == 0:
        raise ValueError("❌ Input dataframe is empty! Something went wrong in earlier steps.")
    
    # Columns to drop for modeling
    drop_cols = [
        'popularity', 'verdict',
        'Unnamed: 0', 'artist_name', 'track_name', 'track_id', 'year',
        'release_date'
    ]
    
    # Also drop random columns if they exist
    drop_cols.extend(['random_popularity', 'random_verdict'])
    
    # Drop only columns that exist
    cols_to_drop = [col for col in drop_cols if col in df.columns]
    context.log.info(f"Columns to drop: {cols_to_drop}")
    
    features = df.drop(columns=cols_to_drop)
    context.log.info(f"✅ After dropping columns: {features.shape}")
    context.log.info(f"Remaining columns: {list(features.columns)}")
    
    if len(features) == 0:
        raise ValueError("❌ Features dataframe is empty after dropping columns!")
    
    # One-hot encode genre if it exists
    if 'genre' in features.columns:
        context.log.info(f"🔄 One-hot encoding 'genre' column with {features['genre'].nunique()} unique values")
        one_hot = pd.get_dummies(features['genre'], prefix='genre', dtype=int)
        features = features.drop('genre', axis=1)
        features = pd.concat([features, one_hot], axis=1)
        context.log.info(f"✅ After one-hot encoding genre: {features.shape}")
    else:
        context.log.info("ℹ️  No 'genre' column found")
    
    # Get target
    if 'verdict' not in df.columns:
        raise ValueError("❌ 'verdict' column not found in input dataframe!")
    
    target = df['verdict'].astype(int)
    context.log.info(f"✅ Target shape: {target.shape}")
    context.log.info(f"Target value counts:\n{target.value_counts()}")
    
    # Sort columns for deterministic order
    features = features.sort_index(axis=1)
    
    context.log.info(f"✅ Final features shape: {features.shape}")
    context.log.info(f"Feature columns: {list(features.columns)}")
    
    # Check for NaN values
    nan_count = features.isna().sum().sum()
    if nan_count > 0:
        context.log.warning(f"⚠️  WARNING: Found {nan_count} NaN values in features!")
        context.log.info(f"NaN per column:\n{features.isna().sum()}")
    
    # Save readable copies
    project_root = _find_project_root()
    output_dir = project_root / "data" / "dagster_output"
    output_dir.mkdir(exist_ok=True)
    
    features_path = output_dir / "03_features.csv"
    target_path = output_dir / "03_target.csv"
    feature_names_path = output_dir / "03_feature_names.txt"
    
    features.to_csv(features_path, index=False)
    target.to_csv(target_path, index=False, header=True)
    
    # Save feature names list
    with open(feature_names_path, 'w') as f:
        f.write('\n'.join(features.columns.tolist()))
    
    context.log.info(f"✅ Saved readable copies to: {output_dir}")
    
    feature_set = FeatureSet(features=features, target=target)
    
    return Output(
        feature_set,
        metadata={
            "feature_count": len(features.columns),
            "rows": len(features),
            "feature_names": list(features.columns)[:10],  # First 10 features
            "readable_features": str(features_path),
            "readable_target": str(target_path),
        },
    )

# -----------------------------------------------------------
# 4. Train/Test split
# -----------------------------------------------------------

@asset(
    ins={"feature_set": AssetIn("spotify_features")},
    description="Split features into X_train, X_test, y_train, y_test."
)
def train_test_split(context, feature_set: FeatureSet) -> Output[Dict[str, Any]]:
    """Split data into train and test sets with stratification."""
    
    X_train, X_test, y_train, y_test = sk_train_test_split(
        feature_set.features,
        feature_set.target,
        test_size=0.2,
        random_state=SEED,
        stratify=feature_set.target,
    )
    
    context.log.info(f"Train set: {len(X_train)} samples")
    context.log.info(f"Test set: {len(X_test)} samples")
    context.log.info(f"Train positive %: {y_train.mean() * 100:.2f}%")
    context.log.info(f"Test positive %: {y_test.mean() * 100:.2f}%")
    
    # Save readable copies
    project_root = _find_project_root()
    output_dir = project_root / "data" / "dagster_output"
    output_dir.mkdir(exist_ok=True)
    
    X_train.to_csv(output_dir / "04_X_train.csv", index=False)
    X_test.to_csv(output_dir / "04_X_test.csv", index=False)
    y_train.to_csv(output_dir / "04_y_train.csv", index=False, header=True)
    y_test.to_csv(output_dir / "04_y_test.csv", index=False, header=True)
    
    context.log.info(f"✅ Saved readable copies to: {output_dir}")
    
    split_data = {
        "X_train": X_train,
        "X_test": X_test,
        "y_train": y_train,
        "y_test": y_test,
    }
    
    return Output(
        split_data,
        metadata={
            "train_rows": len(X_train),
            "test_rows": len(X_test),
            "features": X_train.shape[1],
            "train_positive_pct": float(y_train.mean() * 100),
            "test_positive_pct": float(y_test.mean() * 100),
            "readable_files": str(output_dir),
        },
    )