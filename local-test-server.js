const express = require('express');
const cors = require('cors');
const app = express();
const port = 3001;

// Enable CORS for all origins
app.use(cors());
app.use(express.json());

console.log('Starting HAHD Test Server on localhost:3001');

// Test endpoints that match your current API
app.get('/health', (req, res) => {
    console.log('Health check requested');
    res.json({ 
        status: 'OK', 
        message: 'HAHD Local Test Server is running', 
        timestamp: new Date().toISOString(),
        server: 'localhost'
    });
});

app.get('/', (req, res) => {
    res.send('HAHD Local Test Server is running! API endpoints: /health, /api/videos/*, /auth/*, /survey/*');
});

// Mock S3 video endpoints
app.get('/api/videos/random', (req, res) => {
    console.log('🎥 Random video requested');
    res.json({ 
        message: 'Random video endpoint - test response', 
        url: 'https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4',
        videoId: 'test-random-video',
        status: 'test'
    });
});

app.get('/api/videos/238', (req, res) => {
    console.log('🎥 Video 238 requested');
    res.json({ 
        message: 'Video 238 endpoint - test response', 
        url: 'https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4',
        videoId: 'video238',
        status: 'test'
    });
});

app.get('/api/videos/test-s3', (req, res) => {
    console.log('☁️ S3 test requested');
    res.json({ 
        message: 'S3 connection test - working locally', 
        status: 'test',
        connected: true
    });
});

// Mock auth endpoints
app.post('/auth/signIn', (req, res) => {
    console.log('🔐 SignIn request:', req.body.email);
    res.json({ 
        message: 'Test sign in successful',
        email: req.body.email,
        token: 'test-jwt-token-' + Date.now(),
        surveysCompleted: 2,
        referralCode: 'TEST123',
        numRaffleEntries: 5,
        status: 'test'
    });
});

app.post('/auth/register', (req, res) => {
    console.log('📝 Register request:', req.body.email);
    res.json({ 
        message: 'Test registration successful',
        email: req.body.email,
        token: 'test-jwt-token-' + Date.now(),
        surveysCompleted: 0,
        referralCode: 'TEST456',
        numRaffleEntries: 1,
        status: 'test'
    });
});

app.post('/auth/validateReferral', (req, res) => {
    console.log('🎫 Validate referral request:', req.body.code);
    res.json({ 
        isValid: req.body.code !== 'INVALID',
        message: 'Test referral validation',
        status: 'test'
    });
});

// Mock survey endpoints
app.get('/survey/top-raffle-entries', (req, res) => {
    console.log('🏆 Leaderboard requested for:', req.query.currentUserEmail);
    res.json({ 
        topUsers: [
            { email: 'test1@example.com', numRaffleEntries: 15 },
            { email: 'test2@example.com', numRaffleEntries: 12 },
            { email: 'test3@example.com', numRaffleEntries: 10 },
            { email: req.query.currentUserEmail, numRaffleEntries: 8 },
            { email: 'test4@example.com', numRaffleEntries: 6 }
        ],
        currentUserRank: 4,
        currentUser: { email: req.query.currentUserEmail, numRaffleEntries: 8 },
        status: 'test'
    });
});

app.post('/survey/results', (req, res) => {
    console.log('📊 Survey results submitted:', {
        userId: req.body.userId,
        videoId: req.body.videoId,
        gazePoints: req.body.gaze ? req.body.gaze.length : 0,
        hasFormData: !!req.body.formData
    });
    res.status(201).json({ 
        message: 'Test survey result saved successfully',
        status: 'test'
    });
});

app.listen(port, () => {
    console.log(`
HAHD Local Test Server is running!

📍 Server URL: http://localhost:${port}
🔗 Health Check: http://localhost:${port}/health
🎥 Video API: http://localhost:${port}/api/videos/random
🔐 Auth API: http://localhost:${port}/auth/signIn
📊 Survey API: http://localhost:${port}/survey/results

✨ All API endpoints are mocked and ready for frontend testing
Start your React frontend and test the API integration!
    `);
});