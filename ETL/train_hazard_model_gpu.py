#!/usr/bin/env python3
"""
GPU-Accelerated Hazard Detection Model Training from Gold Tier Data
Uses PyTorch and XGBoost with CUDA for faster training on large datasets
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
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import matplotlib.pyplot as plt
import seaborn as sns

# GPU-accelerated libraries
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader, TensorDataset
import xgboost as xgb

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HazardDataset(Dataset):
    """PyTorch Dataset for hazard detection"""
    
    def __init__(self, X, y):
        self.X = torch.FloatTensor(X.values)
        self.y = torch.LongTensor(y.values)
    
    def __len__(self):
        return len(self.X)
    
    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

class HazardNeuralNet(nn.Module):
    """Neural network for hazard detection with dropout and batch norm"""
    
    def __init__(self, input_size, hidden_sizes=[512, 256, 128, 64], dropout_rate=0.3):
        super(HazardNeuralNet, self).__init__()
        
        layers = []
        prev_size = input_size
        
        for hidden_size in hidden_sizes:
            layers.extend([
                nn.Linear(prev_size, hidden_size),
                nn.BatchNorm1d(hidden_size),
                nn.ReLU(),
                nn.Dropout(dropout_rate)
            ])
            prev_size = hidden_size
        
        # Output layer
        layers.append(nn.Linear(prev_size, 2))  # Binary classification
        
        self.network = nn.Sequential(*layers)
    
    def forward(self, x):
        return self.network(x)

class GPUHazardModelTrainer:
    """Train hazard detection models using GPU acceleration"""
    
    def __init__(self, bucket_name: str):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        logger.info(f"Starting: Using device: {self.device}")
        
        # Check GPU availability
        if torch.cuda.is_available():
            logger.info(f"GPU: {torch.cuda.get_device_name(0)}")
            logger.info(f"CUDA Version: {torch.version.cuda}")
            logger.info(f"GPU Memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.1f} GB")
        
        self.pytorch_model = None
        self.xgb_model = None
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
    
    def train_pytorch_model(self, X: pd.DataFrame, y: pd.Series) -> dict:
        """Train PyTorch neural network with GPU acceleration"""
        logger.info("Starting: Training PyTorch Neural Network on GPU...")
        
        # Check for class imbalance
        class_counts = y.value_counts()
        if len(class_counts) < 2:
            logger.error("Only one class present in target variable. Cannot train model.")
            return {}
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Create datasets and data loaders
        train_dataset = HazardDataset(X_train, y_train)
        test_dataset = HazardDataset(X_test, y_test)
        
        train_loader = DataLoader(train_dataset, batch_size=256, shuffle=True)
        test_loader = DataLoader(test_dataset, batch_size=256, shuffle=False)
        
        # Initialize model
        input_size = X.shape[1]
        self.pytorch_model = HazardNeuralNet(input_size).to(self.device)
        
        # Loss function with class weights for imbalanced data
        class_weights = torch.tensor([
            len(y) / (2 * (y == 0).sum()),  # Weight for class 0
            len(y) / (2 * (y == 1).sum())   # Weight for class 1
        ], dtype=torch.float).to(self.device)
        
        criterion = nn.CrossEntropyLoss(weight=class_weights)
        optimizer = optim.Adam(self.pytorch_model.parameters(), lr=0.001, weight_decay=1e-5)
        scheduler = optim.lr_scheduler.StepLR(optimizer, step_size=20, gamma=0.5)
        
        # Training loop
        num_epochs = 100
        best_val_acc = 0
        
        for epoch in range(num_epochs):
            # Training phase
            self.pytorch_model.train()
            train_loss = 0
            for batch_X, batch_y in train_loader:
                batch_X, batch_y = batch_X.to(self.device), batch_y.to(self.device)
                
                optimizer.zero_grad()
                outputs = self.pytorch_model(batch_X)
                loss = criterion(outputs, batch_y)
                loss.backward()
                optimizer.step()
                
                train_loss += loss.item()
            
            # Validation phase
            self.pytorch_model.eval()
            correct = 0
            total = 0
            with torch.no_grad():
                for batch_X, batch_y in test_loader:
                    batch_X, batch_y = batch_X.to(self.device), batch_y.to(self.device)
                    outputs = self.pytorch_model(batch_X)
                    _, predicted = torch.max(outputs.data, 1)
                    total += batch_y.size(0)
                    correct += (predicted == batch_y).sum().item()
            
            val_acc = correct / total
            scheduler.step()
            
            if epoch % 10 == 0:
                logger.info(f"Epoch {epoch}: Loss={train_loss/len(train_loader):.4f}, Val_Acc={val_acc:.4f}")
            
            if val_acc > best_val_acc:
                best_val_acc = val_acc
        
        # Final evaluation
        self.pytorch_model.eval()
        y_pred = []
        y_true = []
        y_pred_proba = []
        
        with torch.no_grad():
            for batch_X, batch_y in test_loader:
                batch_X, batch_y = batch_X.to(self.device), batch_y.to(self.device)
                outputs = self.pytorch_model(batch_X)
                probs = torch.softmax(outputs, dim=1)
                _, predicted = torch.max(outputs, 1)
                
                y_pred.extend(predicted.cpu().numpy())
                y_true.extend(batch_y.cpu().numpy())
                y_pred_proba.extend(probs[:, 1].cpu().numpy())  # Probability of hazard class
        
        # Metrics
        test_acc = (np.array(y_pred) == np.array(y_true)).mean()
        roc_score = roc_auc_score(y_true, y_pred_proba)
        cm = confusion_matrix(y_true, y_pred)
        
        logger.info(" PyTorch Model Results:")
        logger.info(f"Test Accuracy: {test_acc:.4f}")
        logger.info(f"ROC-AUC Score: {roc_score:.4f}")
        logger.info("Classification Report:")
        print(classification_report(y_true, y_pred, target_names=['Non-Hazard', 'Hazard']))
        
        return {
            'model_type': 'PyTorch',
            'test_accuracy': test_acc,
            'roc_auc': roc_score,
            'confusion_matrix': cm.tolist()
        }
    
    def train_xgboost_gpu_model(self, X: pd.DataFrame, y: pd.Series) -> dict:
        """Train XGBoost model with GPU acceleration"""
        logger.info("Starting: Training XGBoost Model on GPU...")
        
        # Check for class imbalance
        class_counts = y.value_counts()
        if len(class_counts) < 2:
            logger.error("Only one class present in target variable. Cannot train model.")
            return {}
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Calculate class weights for imbalanced data
        scale_pos_weight = (y == 0).sum() / (y == 1).sum()
        
        # XGBoost with GPU
        self.xgb_model = xgb.XGBClassifier(
            tree_method='gpu_hist',  # GPU acceleration
            gpu_id=0,
            n_estimators=500,
            max_depth=8,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,  # Handle class imbalance
            random_state=42,
            eval_metric='auc'
        )
        
        # Train model
        self.xgb_model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            early_stopping_rounds=20,
            verbose=False
        )
        
        # Evaluate model
        y_pred = self.xgb_model.predict(X_test)
        y_pred_proba = self.xgb_model.predict_proba(X_test)[:, 1]
        
        test_acc = (y_pred == y_test).mean()
        roc_score = roc_auc_score(y_test, y_pred_proba)
        cm = confusion_matrix(y_test, y_pred)
        
        # Feature importance
        feature_importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.xgb_model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        logger.info(" XGBoost GPU Model Results:")
        logger.info(f"Test Accuracy: {test_acc:.4f}")
        logger.info(f"ROC-AUC Score: {roc_score:.4f}")
        logger.info(" Top 10 Most Important Features:")
        for idx, row in feature_importance.head(10).iterrows():
            logger.info(f"  {row['feature']}: {row['importance']:.4f}")
        
        return {
            'model_type': 'XGBoost-GPU',
            'test_accuracy': test_acc,
            'roc_auc': roc_score,
            'confusion_matrix': cm.tolist(),
            'feature_importance': feature_importance.to_dict('records')
        }
    
    def save_models(self, pytorch_metrics: dict, xgb_metrics: dict) -> str:
        """Save both trained models to S3"""
        simulation_batch = os.environ.get('SIMULATION_BATCH', '0')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save PyTorch model
        if self.pytorch_model is not None:
            pytorch_model_path = f"/tmp/hazard_detection_pytorch_{timestamp}.pth"
            torch.save({
                'model_state_dict': self.pytorch_model.state_dict(),
                'model_class': 'HazardNeuralNet',
                'input_size': len(self.feature_names),
                'feature_names': self.feature_names,
                'metrics': pytorch_metrics,
                'timestamp': timestamp
            }, pytorch_model_path)
            
            # Upload to S3
            batch_dir = f"models/{simulation_batch}"
            s3_key_pytorch = f"{batch_dir}/hazard_detection_pytorch_{timestamp}.pth"
            self.s3.upload_file(pytorch_model_path, self.bucket, s3_key_pytorch)
            logger.info(f"Success: PyTorch model uploaded: s3://{self.bucket}/{s3_key_pytorch}")
            os.remove(pytorch_model_path)
        
        # Save XGBoost model
        if self.xgb_model is not None:
            xgb_model_path = f"/tmp/hazard_detection_xgboost_{timestamp}.pkl"
            xgb_data = {
                'model': self.xgb_model,
                'feature_names': self.feature_names,
                'metrics': xgb_metrics,
                'timestamp': timestamp,
                'model_type': 'XGBoost-GPU'
            }
            joblib.dump(xgb_data, xgb_model_path)
            
            # Upload to S3
            s3_key_xgb = f"{batch_dir}/hazard_detection_xgboost_{timestamp}.pkl"
            self.s3.upload_file(xgb_model_path, self.bucket, s3_key_xgb)
            logger.info(f"Success: XGBoost model uploaded: s3://{self.bucket}/{s3_key_xgb}")
            os.remove(xgb_model_path)
        
        # Save ensemble model combining both
        ensemble_path = f"/tmp/hazard_detection_ensemble_{timestamp}.pkl"
        ensemble_data = {
            'pytorch_metrics': pytorch_metrics,
            'xgboost_metrics': xgb_metrics,
            'feature_names': self.feature_names,
            'timestamp': timestamp,
            'simulation_batch': simulation_batch,
            'model_type': 'GPU-Ensemble'
        }
        joblib.dump(ensemble_data, ensemble_path)
        
        s3_key_ensemble = f"{batch_dir}/hazard_detection_ensemble_{timestamp}.pkl"
        self.s3.upload_file(ensemble_path, self.bucket, s3_key_ensemble)
        logger.info(f"Success: Ensemble model uploaded: s3://{self.bucket}/{s3_key_ensemble}")
        os.remove(ensemble_path)
        
        return s3_key_ensemble


def main():
    bucket_name = os.environ.get('BUCKET_NAME', 'hahd-primary-data-storage')
    
    logger.info(" Starting GPU-Accelerated Hazard Detection Model Training")
    logger.info("=" * 60)
    
    # Initialize trainer
    trainer = GPUHazardModelTrainer(bucket_name)
    
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
    
    # Train models
    logger.info("Step 3: Training GPU-accelerated models...")
    
    # Train PyTorch model
    pytorch_metrics = trainer.train_pytorch_model(X, y)
    
    # Train XGBoost model
    xgb_metrics = trainer.train_xgboost_gpu_model(X, y)
    
    if not pytorch_metrics and not xgb_metrics:
        logger.error("Both model training attempts failed.")
        return False
    
    # Save models
    logger.info("Step 4: Saving models...")
    model_path = trainer.save_models(pytorch_metrics, xgb_metrics)
    
    if model_path:
        logger.info("Complete: GPU Model Training Complete!")
        logger.info("=" * 60)
        logger.info(f"Models saved to: s3://{bucket_name}/{model_path}")
        if pytorch_metrics:
            logger.info(f"PyTorch ROC-AUC: {pytorch_metrics['roc_auc']:.3f}")
        if xgb_metrics:
            logger.info(f"XGBoost ROC-AUC: {xgb_metrics['roc_auc']:.3f}")
        return True
    else:
        logger.error("Failed to save models")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)