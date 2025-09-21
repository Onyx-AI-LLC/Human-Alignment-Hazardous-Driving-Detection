const express = require('express');
const { createToken } = require('../utils/token');
const { saveSingleUserToS3 } = require('../services/dataStorage');
const router = express.Router();
const User = require('../models/user');

router.post('/register', async (req, res) => {
    const { email, password, referredByUser, ...formData } = req.body;

    try {
        const { user, referralCode } = await User.register(email, password, referredByUser, formData)
        const token = createToken(user._id)
        
        // Immediately save user to S3 primary storage
        try {
            await saveSingleUserToS3(user);
        } catch (s3Error) {
            console.error('Warning: Failed to save user to S3:', s3Error);
            // Don't fail registration if S3 save fails
        }
        
        const surveysCompleted = user.numSurveysFilled
        const numRaffleEntries = user.numRaffleEntries
        
        res.status(200).json({ email, surveysCompleted, referralCode, token, numRaffleEntries})
    } catch (err) {
        if (err.statusCode) {
            res.status(err.statusCode).json({ message: err.message });
        } else {
            res.status(500).json({ message: 'An unexpected error occurred' });
        }
    }
});

router.post('/signIn', async (req, res) => {
    const { email, password } = req.body;
    try {
        const { user, surveysCompleted, referralCode, numRaffleEntries } = await User.signIn(email, password)
        const token = createToken(user._id)

        res.status(200).json({ email, surveysCompleted, referralCode, token, numRaffleEntries})
    } catch (err) {
        res.status(400).json({ err: err.message })
    }
});

router.post('/validateReferral', async (req, res) => {
    const { code } = req.body;
    try {
        const { isValid } = await User.validateReferral(code)

        res.status(200).json({ isValid });
    } catch (err) {
        res.status(400).json({ err: err.message });
    }
})

module.exports = router;