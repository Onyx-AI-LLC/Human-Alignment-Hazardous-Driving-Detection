#!/bin/bash
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "Starting simple HAHD server setup at $(date)"

yum update -y
yum install -y nodejs npm git nginx

echo "Node.js version: $(node --version)"
echo "NPM version: $(npm --version)"

# Install PM2 globally
npm install -g pm2

# Create application directory
mkdir -p /opt/hahd/{server,logs}
cd /opt/hahd

# Create a simple test server that responds like your current API
cat > /opt/hahd/server/server.js << 'EOF'
const express = require('express');
const cors = require('cors');
const app = express();
const port = 3001;

// Enable CORS for all origins
app.use(cors());
app.use(express.json());

// Test endpoints that match your current API
app.get('/health', (req, res) => {
    res.json({ 
        status: 'OK', 
        message: 'HAHD EC2 Server is running', 
        timestamp: new Date().toISOString(),
        server: 'EC2'
    });
});

app.get('/', (req, res) => {
    res.send('HAHD EC2 Server is running! API endpoints: /health, /api/videos/*, /auth/*, /survey/*');
});

// Mock S3 video endpoints
app.get('/api/videos/random', (req, res) => {
    res.json({ 
        message: 'Random video endpoint - mock response', 
        url: 'https://example.com/mock-video.mp4',
        videoId: 'mock-random-video',
        status: 'mock'
    });
});

app.get('/api/videos/238', (req, res) => {
    res.json({ 
        message: 'Video 238 endpoint - mock response', 
        url: 'https://example.com/mock-video238.mp4',
        videoId: 'video238',
        status: 'mock'
    });
});

app.get('/api/videos/test-s3', (req, res) => {
    res.json({ 
        message: 'S3 connection test - mock response', 
        status: 'mock',
        connected: true
    });
});

// Mock auth endpoints
app.post('/auth/signIn', (req, res) => {
    console.log('SignIn request:', req.body);
    res.json({ 
        message: 'Mock sign in successful',
        email: req.body.email,
        token: 'mock-jwt-token',
        surveysCompleted: 0,
        referralCode: 'MOCK123',
        numRaffleEntries: 1,
        status: 'mock'
    });
});

app.post('/auth/register', (req, res) => {
    console.log('Register request:', req.body);
    res.json({ 
        message: 'Mock registration successful',
        email: req.body.email,
        token: 'mock-jwt-token',
        surveysCompleted: 0,
        referralCode: 'MOCK123',
        numRaffleEntries: 1,
        status: 'mock'
    });
});

app.post('/auth/validateReferral', (req, res) => {
    console.log('Validate referral request:', req.body);
    res.json({ 
        isValid: true,
        message: 'Mock referral validation',
        status: 'mock'
    });
});

// Mock survey endpoints
app.get('/survey/top-raffle-entries', (req, res) => {
    res.json({ 
        topUsers: [
            { email: 'test1@example.com', numRaffleEntries: 10 },
            { email: 'test2@example.com', numRaffleEntries: 8 },
            { email: 'test3@example.com', numRaffleEntries: 6 }
        ],
        currentUserRank: 1,
        currentUser: { email: req.query.currentUserEmail, numRaffleEntries: 1 },
        status: 'mock'
    });
});

app.post('/survey/results', (req, res) => {
    console.log('Survey results submission:', {
        userId: req.body.userId,
        videoId: req.body.videoId,
        gazePoints: req.body.gaze ? req.body.gaze.length : 0,
        formData: req.body.formData
    });
    res.status(201).json({ 
        message: 'Mock survey result saved successfully',
        status: 'mock'
    });
});

app.listen(port, () => {
    console.log(`HAHD EC2 test server running on port ${port}`);
    console.log('Mock API endpoints available');
});
EOF

# Install dependencies
cd /opt/hahd/server
npm init -y
npm install express cors

# Create PM2 configuration
cat > /opt/hahd/ecosystem.config.js << 'EOF'
module.exports = {
  apps: [{
    name: 'hahd-server',
    script: 'server.js',
    cwd: '/opt/hahd/server',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '200M',
    env: {
      NODE_ENV: 'production',
      PORT: 3001
    },
    error_file: '/opt/hahd/logs/err.log',
    out_file: '/opt/hahd/logs/out.log',
    log_file: '/opt/hahd/logs/combined.log',
    time: true
  }]
};
EOF

# Set ownership
chown -R ec2-user:ec2-user /opt/hahd

# Configure Nginx
cat > /etc/nginx/nginx.conf << 'EOF'
user nginx;
worker_processes auto;
error_log /var/log/nginx/error.log;
pid /run/nginx.pid;

events {
    worker_connections 1024;
}

http {
    sendfile on;
    tcp_nopush on;
    tcp_nodelay on;
    keepalive_timeout 65;
    types_hash_max_size 2048;
    client_max_body_size 100M;
    
    include /etc/nginx/mime.types;
    default_type application/octet-stream;
    
    upstream nodejs_backend {
        server 127.0.0.1:3001;
    }
    
    server {
        listen 80 default_server;
        server_name _;
        
        # CORS headers
        add_header 'Access-Control-Allow-Origin' '*' always;
        add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS' always;
        add_header 'Access-Control-Allow-Headers' 'DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range,Authorization' always;
        
        # Handle preflight requests
        location / {
            if ($request_method = 'OPTIONS') {
                add_header 'Access-Control-Allow-Origin' '*';
                add_header 'Access-Control-Allow-Methods' 'GET, POST, OPTIONS';
                add_header 'Access-Control-Allow-Headers' 'DNT,User-Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Range,Authorization';
                add_header 'Access-Control-Max-Age' 1728000;
                add_header 'Content-Type' 'text/plain; charset=utf-8';
                add_header 'Content-Length' 0;
                return 204;
            }
            
            proxy_pass http://nodejs_backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }
}
EOF

# Start Nginx
systemctl enable nginx
systemctl start nginx

# Start Node.js application
sudo -u ec2-user bash -c '
    cd /opt/hahd
    export PATH=$PATH:/usr/bin:/usr/local/bin
    pm2 start ecosystem.config.js
    pm2 save
'

# Setup PM2 to start on boot
env PATH=$PATH:/usr/bin /usr/lib/node_modules/pm2/bin/pm2 startup systemd -u ec2-user --hp /home/ec2-user

echo "HAHD simple server setup completed at $(date)"
echo "Server should be available at http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)"