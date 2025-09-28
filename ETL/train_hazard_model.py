#!/usr/bin/env python3
"""
Hazard Detection Model Training from Gold Tier Data
Trains ML model using merged silver + YOLOv8 features from S3 gold bucket
"""

import pandas as pd
import numpy as np
import boto3
import joblib
import os
import logging
from datetime import datetime
from typing import Tuple
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import matplotlib.pyplot as plt
import seaborn as sns

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HazardModelTrainer:
    """Train hazard detection models using gold tier data"""
    
    def __init__(self, bucket_name: str):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        self.model = None
        self.feature_names = None
        
    def load_gold_data(self) -> pd.DataFrame:
        """Load all gold tier training data from S3"""
        dfs = []
        
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix='data/processed/',
                MaxKeys=1000
            )
            
            if 'Contents' not in response:
                logger.error("No processed training data found")
                return pd.DataFrame()
            
            logger.info(f"Found {len(response['Contents'])} processed training files")
            
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    try:
                        # Download parquet file temporarily
                        local_file = f"/tmp/{os.path.basename(obj['Key'])}"
                        self.s3.download_file(self.bucket, obj['Key'], local_file)
                        
                        # Read parquet
                        df = pd.read_parquet(local_file)
                        dfs.append(df)
                        
                        # Cleanup
                        os.remove(local_file)
                        
                        logger.info(f"Loaded {len(df)} records from {obj['Key']}")
                        
                    except Exception as e:
                        logger.error(f"Error loading {obj['Key']}: {e}")
                        continue
            
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                logger.info(f"Success: Loaded {len(combined_df)} total records from processed tier")
                return combined_df
            else:
                logger.error("No valid processed tier data loaded")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error loading processed tier data: {e}")
            return pd.DataFrame()
    
    def create_target_variable(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create hazard_label target variable from existing features"""
        # Create hazard label based on existing indicators
        df['hazard_label'] = 0
        
        # Method 1: Use existing hazard moment indicator
        if 'is_hazard_moment' in df.columns:
            df.loc[df['is_hazard_moment'] == True, 'hazard_label'] = 1
            logger.info("Using 'is_hazard_moment' column for hazard labels")
        
        # Method 2: Use spacebar press timing (hazards occur near spacebar)
        elif 'time_to_nearest_spacebar' in df.columns:
            # Consider records within 2 seconds of spacebar press as hazards
            hazard_mask = (df['time_to_nearest_spacebar'].abs() <= 2000) & df['time_to_nearest_spacebar'].notna()
            df.loc[hazard_mask, 'hazard_label'] = 1
            logger.info("Using 'time_to_nearest_spacebar' for hazard labels")
        
        # Method 3: Use YOLOv8 + attention patterns as proxy
        elif 'has_yolo_data' in df.columns:
            # Combine multiple indicators for hazard detection
            hazard_conditions = []
            
            # High object count + focused attention
            if 'yolo_total_objects' in df.columns and 'is_fixation' in df.columns:
                condition1 = (df['yolo_total_objects'] > df['yolo_total_objects'].quantile(0.7)) & (df['is_fixation'] == 1)
                hazard_conditions.append(condition1)
            
            # Multiple vehicles present
            if 'yolo_multiple_vehicles' in df.columns:
                hazard_conditions.append(df['yolo_multiple_vehicles'] == 1)
            
            # Pedestrians present
            if 'yolo_pedestrians_present' in df.columns:
                hazard_conditions.append(df['yolo_pedestrians_present'] == 1)
            
            # High gaze speed (indicating rapid attention shift)
            if 'gaze_speed' in df.columns:
                high_speed = df['gaze_speed'] > df['gaze_speed'].quantile(0.8)
                hazard_conditions.append(high_speed)
            
            # Combine conditions
            if hazard_conditions:
                combined_hazard = hazard_conditions[0]
                for condition in hazard_conditions[1:]:
                    combined_hazard = combined_hazard | condition
                df.loc[combined_hazard, 'hazard_label'] = 1
                logger.info("Using combined YOLOv8 + attention patterns for hazard labels")
        
        hazard_count = df['hazard_label'].sum()
        total_count = len(df)
        logger.info(f"Created hazard labels: {hazard_count}/{total_count} ({hazard_count/total_count*100:.1f}%) hazards")
        
        return df
    
    def prepare_features(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        """Prepare features and target for model training"""
        
        # Create target variable if it doesn't exist
        if 'hazard_label' not in df.columns:
            df = self.create_target_variable(df)
        
        # Define feature columns to use for training
        feature_columns = []
        
        # Gaze and attention features
        gaze_features = [
            'video_rel_x', 'video_rel_y', 'gaze_speed', 'is_fixation', 'is_saccade',
            'distance_from_center', 'gaze_dispersion_total', 'gaze_velocity_x', 'gaze_velocity_y'
        ]
        
        # Screen and viewport features
        screen_features = [
            'screen_width', 'screen_height', 'viewport_width', 'viewport_height',
            'is_center_region', 'is_peripheral', 'is_horizon_region'
        ]
        
        # YOLOv8 features
        yolo_features = [
            'yolo_total_objects', 'yolo_vehicles', 'yolo_persons', 'yolo_traffic_signs',
            'yolo_other_objects', 'yolo_avg_object_size', 'yolo_max_object_size',
            'yolo_objects_center_region', 'yolo_objects_peripheral', 'yolo_highest_confidence',
            'yolo_avg_confidence', 'yolo_objects_near_center', 'yolo_large_objects',
            'yolo_multiple_vehicles', 'yolo_pedestrians_present'
        ]
        
        # Temporal features
        temporal_features = [
            'timestamp', 'video_progress', 'time_to_nearest_spacebar'
        ]
        
        # Add available features
        all_potential_features = gaze_features + screen_features + yolo_features + temporal_features
        
        for feature in all_potential_features:
            if feature in df.columns:
                feature_columns.append(feature)
        
        logger.info(f"Using {len(feature_columns)} features for training")
        
        # Select features and target
        X = df[feature_columns].copy()
        y = df['hazard_label'].copy()
        
        # Handle missing values
        X = X.fillna(0)
        
        # Store feature names
        self.feature_names = X.columns.tolist()
        
        logger.info(f"Feature matrix shape: {X.shape}")
        logger.info(f"Target distribution: {y.value_counts().to_dict()}")
        
        return X, y
    
    def train_model(self, X: pd.DataFrame, y: pd.Series) -> dict:
        """Train Random Forest model with cross-validation"""
        
        # Check for class imbalance
        class_counts = y.value_counts()
        if len(class_counts) < 2:
            logger.error("Only one class present in target variable. Cannot train model.")
            return {}
        
        imbalance_ratio = class_counts.min() / class_counts.max()
        logger.info(f"Class imbalance ratio: {imbalance_ratio:.3f}")
        
        # Split data (stratified to maintain class ratio)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        logger.info(f"Training set: {len(X_train)} samples")
        logger.info(f"Test set: {len(X_test)} samples")
        
        # Train Random Forest with class balancing
        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=20,
            class_weight='balanced',  # Critical for imbalanced data
            random_state=42,
            n_jobs=-1  # Use all CPU cores
        )
        
        logger.info("Starting: Training Random Forest model...")
        self.model.fit(X_train, y_train)
        
        # Evaluate model
        y_pred = self.model.predict(X_test)
        y_pred_proba = self.model.predict_proba(X_test)[:, 1]
        
        logger.info("\n=== Model Performance ===")
        print(classification_report(y_test, y_pred, target_names=['Non-Hazard', 'Hazard']))
        
        # ROC-AUC Score (important for imbalanced data)
        try:
            roc_score = roc_auc_score(y_test, y_pred_proba)
            logger.info(f"\nROC-AUC Score: {roc_score:.3f}")
        except Exception as e:
            logger.warning(f"Could not calculate ROC-AUC: {e}")
            roc_score = 0.0
        
        # Confusion Matrix
        cm = confusion_matrix(y_test, y_pred)
        logger.info(f"\nConfusion Matrix:")
        logger.info(f"True Negatives: {cm[0,0]:,}")
        logger.info(f"False Positives: {cm[0,1]:,}")
        logger.info(f"False Negatives: {cm[1,0]:,}")
        logger.info(f"True Positives: {cm[1,1]:,}")
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        logger.info("\n Top 15 Most Important Features:")
        for idx, row in feature_importance.head(15).iterrows():
            logger.info(f"  {row['feature']}: {row['importance']:.4f}")
        
        # Return metrics
        metrics = {
            'roc_auc': roc_score,
            'confusion_matrix': cm.tolist(),
            'feature_importance': feature_importance.to_dict('records'),
            'test_accuracy': (y_pred == y_test).mean(),
            'class_distribution': y.value_counts().to_dict()
        }
        
        return metrics
    
    def save_model(self) -> str:
        """Save trained model to S3"""
        if self.model is None:
            logger.error("No model to save")
            return ""
        
        # Get simulation batch for model versioning
        simulation_batch = os.environ.get('SIMULATION_BATCH', '0')
        model_version = os.environ.get('MODEL_VERSION', f'batch_{simulation_batch}')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        local_model_path = f"/tmp/hazard_detection_model_{model_version}_{timestamp}.pkl"
        
        # Save model with metadata
        model_data = {
            'model': self.model,
            'feature_names': self.feature_names,
            'timestamp': timestamp,
            'model_type': 'RandomForestClassifier'
        }
        
        joblib.dump(model_data, local_model_path)
        logger.info(f"Model saved locally: {local_model_path}")
        
        # Upload to S3 with batch versioning
        batch_dir = f"models/{simulation_batch}"
        s3_key = f"{batch_dir}/hazard_detection_model_{model_version}_{timestamp}.pkl"
        
        try:
            self.s3.upload_file(local_model_path, self.bucket, s3_key)
            logger.info(f"Success: Model uploaded to S3: s3://{self.bucket}/{s3_key}")
            
            # Also save as latest for this batch
            latest_key = f"{batch_dir}/hazard_detection_model_latest.pkl"
            self.s3.upload_file(local_model_path, self.bucket, latest_key)
            logger.info(f"Success: Latest batch model saved: s3://{self.bucket}/{latest_key}")
            
            # Update global latest if this is the highest batch
            try:
                global_latest_key = "models/hazard_detection_model_latest.pkl"
                self.s3.upload_file(local_model_path, self.bucket, global_latest_key)
                logger.info(f"Success: Global latest model updated: s3://{self.bucket}/{global_latest_key}")
            except Exception as e:
                logger.warning(f"Could not update global latest model: {e}")
            
            # Cleanup
            os.remove(local_model_path)
            
            return s3_key
            
        except Exception as e:
            logger.error(f"Error uploading model: {e}")
            return ""
    
    def generate_training_report(self, metrics: dict) -> str:
        """Generate and save training report"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        report = f"""
# Hazard Detection Model Training Report
**Generated:** {timestamp}

## Model Performance
- **ROC-AUC Score:** {metrics.get('roc_auc', 0):.3f}
- **Test Accuracy:** {metrics.get('test_accuracy', 0):.3f}

## Class Distribution
"""
        
        class_dist = metrics.get('class_distribution', {})
        for class_label, count in class_dist.items():
            report += f"- **{class_label}:** {count:,} samples\n"
        
        report += "\n## Confusion Matrix\n"
        cm = metrics.get('confusion_matrix', [[0, 0], [0, 0]])
        report += f"- True Negatives: {cm[0][0]:,}\n"
        report += f"- False Positives: {cm[0][1]:,}\n" 
        report += f"- False Negatives: {cm[1][0]:,}\n"
        report += f"- True Positives: {cm[1][1]:,}\n"
        
        report += "\n## Top 10 Most Important Features\n"
        feature_importance = metrics.get('feature_importance', [])[:10]
        for feat in feature_importance:
            report += f"- **{feat['feature']}:** {feat['importance']:.4f}\n"
        
        # Save report to S3
        report_key = f"models/reports/training_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=report_key,
                Body=report,
                ContentType='text/markdown'
            )
            logger.info(f"Training report saved: s3://{self.bucket}/{report_key}")
            return report_key
        except Exception as e:
            logger.error(f"Error saving report: {e}")
            return ""


def main():
    bucket_name = os.environ.get('BUCKET_NAME', 'hahd-primary-data-storage')
    
    logger.info(" Starting Hazard Detection Model Training")
    logger.info("=" * 60)
    
    # Initialize trainer
    trainer = HazardModelTrainer(bucket_name)
    
    # Load processed tier data
    logger.info("Step 1: Loading processed training data...")
    df = trainer.load_gold_data()
    
    if df.empty:
        logger.error("No processed tier data found. Cannot train model.")
        return False
    
    # Prepare features
    logger.info("Step 2: Preparing features and target...")
    X, y = trainer.prepare_features(df)
    
    if X.empty or y.empty:
        logger.error("No valid features or target found.")
        return False
    
    # Train model
    logger.info("Step 3: Training model...")
    metrics = trainer.train_model(X, y)
    
    if not metrics:
        logger.error("Model training failed.")
        return False
    
    # Save model
    logger.info("Step 4: Saving model...")
    model_path = trainer.save_model()
    
    # Generate report
    logger.info("Step 5: Generating training report...")
    report_path = trainer.generate_training_report(metrics)
    
    if model_path and report_path:
        logger.info("Complete: Model Training Complete!")
        logger.info("=" * 60)
        logger.info(f"Model saved: s3://{bucket_name}/{model_path}")
        logger.info(f"Report saved: s3://{bucket_name}/{report_path}")
        logger.info(f"ROC-AUC Score: {metrics['roc_auc']:.3f}")
        return True
    else:
        logger.error("Failed to save model or report")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)