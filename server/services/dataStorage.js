const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const User = require('../models/user');
const Survey = require('../models/survey');

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
});

const PRIMARY_DATA_BUCKET = 'hahd-primary-data-storage';

async function saveUsersToS3() {
  try {
    console.log('Starting user data storage to S3...');
    
    // Get all users from MongoDB
    const users = await User.find({}).lean();
    
    // Create backup file with timestamp
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `raw/users-backup-${timestamp}.json`;
    
    // Upload to S3
    const putCommand = new PutObjectCommand({
      Bucket: PRIMARY_DATA_BUCKET,
      Key: fileName,
      Body: JSON.stringify(users, null, 2),
      ContentType: 'application/json'
    });
    
    await s3Client.send(putCommand);
    console.log(`✅ Users backup saved to S3: ${fileName}`);
    
    return { success: true, fileName, count: users.length };
  } catch (error) {
    console.error('❌ Error backing up users to S3:', error);
    throw error;
  }
}

async function saveSurveysToS3() {
  try {
    console.log('Starting survey data storage to S3...');
    
    // Get all surveys from MongoDB
    const surveys = await Survey.find({}).lean();
    
    // Create backup file with timestamp
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `raw/surveys-backup-${timestamp}.json`;
    
    // Upload to S3
    const putCommand = new PutObjectCommand({
      Bucket: PRIMARY_DATA_BUCKET,
      Key: fileName,
      Body: JSON.stringify(surveys, null, 2),
      ContentType: 'application/json'
    });
    
    await s3Client.send(putCommand);
    console.log(`✅ Surveys backup saved to S3: ${fileName}`);
    
    return { success: true, fileName, count: surveys.length };
  } catch (error) {
    console.error('❌ Error backing up surveys to S3:', error);
    throw error;
  }
}

async function saveAllDataToS3() {
  try {
    console.log('🔄 Starting full data storage to S3...');
    
    const userStorage = await saveUsersToS3();
    const surveyStorage = await saveSurveysToS3();
    
    console.log('✅ Full data storage completed!');
    console.log(`- Users: ${userStorage.count} records → ${userStorage.fileName}`);
    console.log(`- Surveys: ${surveyStorage.count} records → ${surveyStorage.fileName}`);
    
    return {
      success: true,
      storage: {
        users: userStorage,
        surveys: surveyStorage
      },
      timestamp: new Date().toISOString()
    };
  } catch (error) {
    console.error('❌ Full data storage failed:', error);
    throw error;
  }
}

// Schedule automatic data storage (every 24 hours)
function startAutomaticDataStorage() {
  console.log('🕒 Starting automatic data storage to S3 (every 24 hours)');
  
  // Store data immediately on startup
  saveAllDataToS3().catch(console.error);
  
  // Then store data every 24 hours
  setInterval(() => {
    saveAllDataToS3().catch(console.error);
  }, 24 * 60 * 60 * 1000); // 24 hours in milliseconds
}

module.exports = {
  saveUsersToS3,
  saveSurveysToS3,
  saveAllDataToS3,
  startAutomaticDataStorage
};