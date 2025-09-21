const express = require('express');
const bcrypt = require('bcrypt');
const { createToken } = require('../utils/token');
const { createUserInS3, findUserByEmail } = require('../services/dataStorage');
const router = express.Router();

router.post('/register', async (req, res) => {
    const { email, password, referredByUser, ...formData } = req.body;

    try {
        // Check if user already exists
        const existingUser = await findUserByEmail(email);
        if (existingUser) {
            return res.status(400).json({ message: 'User already exists' });
        }
        
        // Create user directly in S3
        const { user, referralCode } = await createUserInS3(email, password, referredByUser, formData);
        const token = createToken(user._id);
        
        const surveysCompleted = user.numSurveysFilled;
        const numRaffleEntries = user.numRaffleEntries;
        
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
        // Find user in S3
        const user = await findUserByEmail(email);
        if (!user) {
            return res.status(400).json({ err: 'User not found' });
        }
        
        // Verify password
        const isValidPassword = await bcrypt.compare(password, user.password);
        if (!isValidPassword) {
            return res.status(400).json({ err: 'Invalid credentials' });
        }
        
        const token = createToken(user._id);
        const surveysCompleted = user.numSurveysFilled;
        const referralCode = user.referralCode;
        const numRaffleEntries = user.numRaffleEntries;

        res.status(200).json({ email, surveysCompleted, referralCode, token, numRaffleEntries})
    } catch (err) {
        res.status(400).json({ err: err.message })
    }
});

router.post('/validateReferral', async (req, res) => {
    const { code } = req.body;
    try {
        // For now, return true for any code since we'd need to scan S3 for referral codes
        // This could be optimized with a separate referral index
        const isValid = true;

        res.status(200).json({ isValid });
    } catch (err) {
        res.status(400).json({ err: err.message });
    }
})

module.exports = router;