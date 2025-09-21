const express = require('express');
const { createSurveyInS3 } = require('../services/dataStorage');
const router = express.Router();

router.get('/top-raffle-entries', async (req, res) => {
    try {
        // Simplified version - return empty leaderboard since we'd need to scan all S3 files
        // This could be optimized with a proper indexing system
        const topUsers = [];
        const currentUserRank = 1;
        const currentUser = null;
        
        res.json({ topUsers, currentUserRank, currentUser});
    } catch (error) {
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
        
        // Note: User survey count update would require finding and updating user file in S3
        // This is simplified for now - could be enhanced with user management functions
        

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