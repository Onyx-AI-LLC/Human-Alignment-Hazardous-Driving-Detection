const express = require('express');
const { createSurveyInS3, findUserByEmail, updateUserInS3 } = require('../services/dataStorage');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const router = express.Router();

// S3 configuration
const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
});

const PRIMARY_DATA_BUCKET = 'primary-user-survey-data';

router.get('/top-raffle-entries', async (req, res) => {
    try {
        const currentUserEmail = req.query.currentUserEmail;
        
        // List all user files in S3
        const listCommand = new ListObjectsV2Command({
            Bucket: PRIMARY_DATA_BUCKET,
            Prefix: 'raw/users/',
            MaxKeys: 1000
        });
        
        const response = await s3Client.send(listCommand);
        
        if (!response.Contents || response.Contents.length === 0) {
            return res.json({ topUsers: [], currentUserRank: 1, currentUser: null });
        }
        
        // Read all user files and collect data
        const allUsers = [];
        for (const object of response.Contents) {
            // Skip directory entries and empty files
            if (object.Key.endsWith('/') || object.Size === 0) {
                continue;
            }
            
            try {
                const getCommand = new GetObjectCommand({
                    Bucket: PRIMARY_DATA_BUCKET,
                    Key: object.Key
                });
                
                const fileResponse = await s3Client.send(getCommand);
                const userData = JSON.parse(await fileResponse.Body.transformToString());
                
                allUsers.push({
                    email: userData.email,
                    numRaffleEntries: userData.numRaffleEntries || 0
                });
            } catch (fileError) {
                console.error(`Error reading user file ${object.Key}:`, fileError);
                continue;
            }
        }
        
        // Sort users by numRaffleEntries (highest first)
        allUsers.sort((a, b) => b.numRaffleEntries - a.numRaffleEntries);
        
        // Get top 5 users
        const topUsers = allUsers.slice(0, 5);
        
        // Find current user and their rank
        let currentUser = null;
        let currentUserRank = 1;
        
        if (currentUserEmail) {
            const userIndex = allUsers.findIndex(user => user.email === currentUserEmail);
            if (userIndex !== -1) {
                currentUser = allUsers[userIndex];
                currentUserRank = userIndex + 1;
            }
        }
        
        res.json({ topUsers, currentUserRank, currentUser });
    } catch (error) {
        console.error('Error fetching top users:', error);
        res.status(500).json({ message: 'Error fetching top users' });
    }
});

router.post('/results', async (req, res) => {

    try {
        const body = req.body;
        const { userId, videoId, gaze, windowDimensions, formData, numSurveysCompleted } = body;

        const cleanedGazeData = gaze.map(entry => ({
            time: entry.timestamp,
            x: entry.x,
            y: entry.y
        }));

        // Create survey directly in S3
        await createSurveyInS3(userId, videoId, cleanedGazeData, windowDimensions, formData);
        
        // Update user's survey count and raffle entries in S3
        await updateUserInS3(userId, {
            numSurveysFilled: numSurveysCompleted,
            numRaffleEntries: (await findUserByEmail(userId)).numRaffleEntries + 1
        });
        

        res.status(201).json({ message: 'Survey result saved successfully; User data updated'});
    } catch (err) {

        console.log('An error has occurred while saving results', err)
        res.status(500).json({
            message: 'Error saving survey result',
            error: err.message
        });
    }
})

module.exports = router;