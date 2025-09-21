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

async function saveSingleUserToS3(user) {
  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `raw/user-${user._id}-${timestamp}.json`;
    
    const putCommand = new PutObjectCommand({
      Bucket: PRIMARY_DATA_BUCKET,
      Key: fileName,
      Body: JSON.stringify(user, null, 2),
      ContentType: 'application/json'
    });
    
    await s3Client.send(putCommand);
    console.log(`✅ User ${user.email} saved to S3: ${fileName}`);
    
    return { success: true, fileName, userId: user._id };
  } catch (error) {
    console.error('❌ Error saving user to S3:', error);
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

async function saveSingleSurveyToS3(survey) {
  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `raw/survey-${survey._id}-${timestamp}.json`;
    
    const putCommand = new PutObjectCommand({
      Bucket: PRIMARY_DATA_BUCKET,
      Key: fileName,
      Body: JSON.stringify(survey, null, 2),
      ContentType: 'application/json'
    });
    
    await s3Client.send(putCommand);
    console.log(`✅ Survey ${survey._id} saved to S3: ${fileName}`);
    
    return { success: true, fileName, surveyId: survey._id };
  } catch (error) {
    console.error('❌ Error saving survey to S3:', error);
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

module.exports = {
  saveUsersToS3,
  saveSurveysToS3,
  saveAllDataToS3,
  saveSingleUserToS3,
  saveSingleSurveyToS3
};