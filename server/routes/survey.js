const express = require('express');
const { saveSingleSurveyToS3 } = require('../services/dataStorage');
const router = express.Router();
const SurveyResult = require('../models/survey');
const User = require('../models/user');

router.get('/top-raffle-entries', async (req, res) => {
    try {
        const currentUserEmail = req.query.currentUserEmail;
        const currentUser = await User.findOne({ email: currentUserEmail });

        const topUsers = await User.find()
            .sort({ numRaffleEntries: -1 })
            .limit(5)
            .select('email numRaffleEntries');
        
        // Find the current user's rank
        const currentUserRank = await User.countDocuments({ numRaffleEntries: { $gt: currentUser.numRaffleEntries } }) + 1;
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

        const survey = await SurveyResult.create({
            userId: userId,
            videoId: videoId,
            windowDimensions, windowDimensions,
            gaze: cleanedGazeData,
            formData: formData
        })
        
        // Immediately save survey to S3 primary storage
        try {
            await saveSingleSurveyToS3(survey);
        } catch (s3Error) {
            console.error('Warning: Failed to save survey to S3:', s3Error);
            // Don't fail survey submission if S3 save fails
        }
        
        await User.findOneAndUpdate(
            {email: userId}, 
            {
              $set: { numSurveysFilled: numSurveysCompleted },
              $inc: { numRaffleEntries: 1}
            },
            { new: true }
        );
        

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