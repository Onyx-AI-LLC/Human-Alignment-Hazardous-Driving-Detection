const express = require('express');
const { saveUsersToS3, saveSurveysToS3, saveAllDataToS3, backfillMongoToS3 } = require('../services/dataStorage');
const router = express.Router();

// Manual data storage endpoints
router.post('/users', async (req, res) => {
  try {
    const result = await saveUsersToS3();
    res.status(200).json({
      message: 'User data storage completed successfully',
      ...result
    });
  } catch (error) {
    res.status(500).json({
      message: 'Failed to backup user data',
      error: error.message
    });
  }
});

router.post('/surveys', async (req, res) => {
  try {
    const result = await saveSurveysToS3();
    res.status(200).json({
      message: 'Survey data storage completed successfully',
      ...result
    });
  } catch (error) {
    res.status(500).json({
      message: 'Failed to backup survey data',
      error: error.message
    });
  }
});

router.post('/all', async (req, res) => {
  try {
    const result = await saveAllDataToS3();
    res.status(200).json({
      message: 'Full data storage completed successfully',
      ...result
    });
  } catch (error) {
    res.status(500).json({
      message: 'Failed to complete full backup',
      error: error.message
    });
  }
});

// Status endpoint
router.get('/status', (req, res) => {
  res.status(200).json({
    message: 'Primary data storage service is running',
    bucket: 'hahd-primary-data-storage',
    folder: 'raw',
    automaticStorage: true,
    interval: '24 hours'
  });
});

// Backfill endpoint to migrate existing MongoDB data to S3
router.post('/backfill', async (req, res) => {
  try {
    console.log('🔄 Starting backfill process...');
    const result = await backfillMongoToS3();
    res.status(200).json({
      message: 'MongoDB backfill to S3 completed successfully',
      ...result
    });
  } catch (error) {
    console.error('❌ Backfill failed:', error);
    res.status(500).json({
      message: 'Failed to backfill MongoDB data to S3',
      error: error.message
    });
  }
});

module.exports = router;