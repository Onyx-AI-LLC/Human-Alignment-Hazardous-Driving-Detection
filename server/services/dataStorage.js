const { S3Client, PutObjectCommand, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const bcrypt = require('bcrypt');
const crypto = require('crypto');

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
});

const PRIMARY_DATA_BUCKET = 'hahd-primary-data-storage';

async function saveSingleUserToS3(user) {
  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `raw/users/${user._id}-${timestamp}.json`;
    
    // Convert MongoDB document to plain object, preserving all MongoDB fields
    const userData = user.toObject ? user.toObject() : user;
    
    const putCommand = new PutObjectCommand({
      Bucket: PRIMARY_DATA_BUCKET,
      Key: fileName,
      Body: JSON.stringify(userData, null, 2),
      ContentType: 'application/json'
    });
    
    await s3Client.send(putCommand);
    console.log(`✅ User ${userData.email} saved to S3: ${fileName}`);
    
    return { success: true, fileName, userId: userData._id };
  } catch (error) {
    console.error('❌ Error saving user to S3:', error);
    throw error;
  }
}

async function saveSingleSurveyToS3(survey) {
  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `raw/results/${survey._id}-${timestamp}.json`;
    
    // Convert MongoDB document to plain object, preserving all MongoDB fields
    const surveyData = survey.toObject ? survey.toObject() : survey;
    
    const putCommand = new PutObjectCommand({
      Bucket: PRIMARY_DATA_BUCKET,
      Key: fileName,
      Body: JSON.stringify(surveyData, null, 2),
      ContentType: 'application/json'
    });
    
    await s3Client.send(putCommand);
    console.log(`✅ Survey ${surveyData._id} saved to S3: ${fileName}`);
    
    return { success: true, fileName, surveyId: surveyData._id };
  } catch (error) {
    console.error('❌ Error saving survey to S3:', error);
    throw error;
  }
}

// Generate unique user ID
function generateUserId() {
  return crypto.randomBytes(12).toString('hex');
}

// Generate referral code
function generateReferralCode() {
  return crypto.randomBytes(6).toString('hex').toUpperCase();
}

// Create user directly in S3
async function createUserInS3(email, password, referredByUser, formData) {
  try {
    // Hash password
    const hashedPassword = await bcrypt.hash(password, 12);
    
    // Create user object
    const userId = generateUserId();
    const referralCode = generateReferralCode();
    const timestamp = new Date().toISOString();
    
    const userData = {
      _id: userId,
      email,
      password: hashedPassword,
      referralCode,
      numSurveysFilled: 0,
      numRaffleEntries: 0,
      createdAt: timestamp,
      updatedAt: timestamp,
      referredByUser: referredByUser || null,
      ...formData
    };
    
    const fileName = `raw/users/${userId}-${timestamp.replace(/[:.]/g, '-')}.json`;
    
    const putCommand = new PutObjectCommand({
      Bucket: PRIMARY_DATA_BUCKET,
      Key: fileName,
      Body: JSON.stringify(userData, null, 2),
      ContentType: 'application/json'
    });
    
    await s3Client.send(putCommand);
    console.log(`✅ User ${email} created in S3: ${fileName}`);
    
    return { user: userData, referralCode };
  } catch (error) {
    console.error('❌ Error creating user in S3:', error);
    throw error;
  }
}

// Find user by email in S3
async function findUserByEmail(email) {
  try {
    const listCommand = new ListObjectsV2Command({
      Bucket: PRIMARY_DATA_BUCKET,
      Prefix: 'raw/users/',
      MaxKeys: 1000
    });
    
    const response = await s3Client.send(listCommand);
    
    if (!response.Contents || response.Contents.length === 0) {
      return null;
    }
    
    // Search through user files
    for (const object of response.Contents) {
      try {
        const getCommand = new GetObjectCommand({
          Bucket: PRIMARY_DATA_BUCKET,
          Key: object.Key
        });
        
        const fileResponse = await s3Client.send(getCommand);
        const userData = JSON.parse(await fileResponse.Body.transformToString());
        
        if (userData.email === email) {
          return userData;
        }
      } catch (fileError) {
        console.error(`Error reading user file ${object.Key}:`, fileError);
        // Skip corrupted files and continue searching
        continue;
      }
    }
    
    return null;
  } catch (error) {
    console.error('❌ Error finding user by email:', error);
    throw error;
  }
}

// Create survey directly in S3
async function createSurveyInS3(userId, videoId, gaze, windowDimensions, formData) {
  try {
    const surveyId = generateUserId(); // Reuse function for unique ID
    const timestamp = new Date().toISOString();
    
    const surveyData = {
      _id: surveyId,
      userId,
      videoId,
      gaze,
      windowDimensions,
      formData,
      createdAt: timestamp,
      updatedAt: timestamp
    };
    
    const fileName = `raw/results/${surveyId}-${timestamp.replace(/[:.]/g, '-')}.json`;
    
    const putCommand = new PutObjectCommand({
      Bucket: PRIMARY_DATA_BUCKET,
      Key: fileName,
      Body: JSON.stringify(surveyData, null, 2),
      ContentType: 'application/json'
    });
    
    await s3Client.send(putCommand);
    console.log(`✅ Survey ${surveyId} created in S3: ${fileName}`);
    
    return surveyData;
  } catch (error) {
    console.error('❌ Error creating survey in S3:', error);
    throw error;
  }
}

// Get top users by raffle entries (simplified version)
async function getTopUsers() {
  try {
    // For now, return empty array since we'd need to scan all S3 user files
    // This could be optimized with a database index or caching layer
    return [];
  } catch (error) {
    console.error('❌ Error getting top users:', error);
    throw error;
  }
}

module.exports = {
  saveSingleUserToS3,
  saveSingleSurveyToS3,
  createUserInS3,
  findUserByEmail,
  createSurveyInS3,
  getTopUsers
};