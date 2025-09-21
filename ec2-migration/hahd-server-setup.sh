#!/bin/bash
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "Starting HAHD server setup at $(date)"

yum update -y

# Install Node.js 18 LTS  
echo "Installing Node.js 18..."
curl -fsSL https://rpm.nodesource.com/setup_18.x | bash -
yum install -y nodejs git nginx python3 python3-pip

# Verify Node.js installation
echo "Node.js version: $(node --version)"
echo "NPM version: $(npm --version)"

# Install PM2 globally
echo "Installing PM2..."
npm install -g pm2

# Create application directory
echo "Creating application directory..."
mkdir -p /opt/hahd/{server,frontend,data,logs,ssl,scripts}
cd /opt/hahd

# Clone repository
echo "Cloning repository..."
git clone https://github.com/lennox55555/Human-Alignment-Hazardous-Driving-Detection.git temp-repo

# Check what we got
echo "Repository contents:"
ls -la temp-repo/

# Move server files
echo "Setting up server files..."
if [ -d "temp-repo/server" ]; then
    cp -r temp-repo/server/* /opt/hahd/server/
    echo "Server files copied"
else
    echo "No server directory found in repository"
    ls -la temp-repo/
fi

# Check for frontend build
if [ -d "temp-repo/frontend/dist" ]; then
    cp -r temp-repo/frontend/dist/* /opt/hahd/frontend/
    echo "Frontend files copied"
else
    echo "No frontend/dist directory found"
fi

# Copy ETL if it exists
if [ -d "temp-repo/ETL" ]; then
    cp -r temp-repo/ETL /opt/hahd/data/
    echo "ETL files copied"
fi

# Clean up
rm -rf temp-repo

# Set proper ownership
chown -R ec2-user:ec2-user /opt/hahd

# Install server dependencies
echo "Installing Node.js dependencies..."
cd /opt/hahd/server
if [ -f "package.json" ]; then
    npm install --production
    echo "Dependencies installed"
else
    echo "No package.json found in server directory"
    ls -la /opt/hahd/server/
fi

# Create environment file
echo "Creating environment file..."
cat > /opt/hahd/server/.env << EOL
MONGODB_URI=mongodb+srv://jonahmulcrone:1RFqimbiYxCpSlkU@hahdcluster.igin9.mongodb.net/survey?retryWrites=true&w=majority&appName=HAHDCluster
SECRET=hahd-jwt-secret-$(date +%s)
AWS_REGION=us-east-1
S3_BUCKET_NAME=driving-simulator-videos
PORT=3001
NODE_ENV=production
EOL

echo "Environment file created"

# Create PM2 ecosystem file
echo "Creating PM2 configuration..."
cat > /opt/hahd/ecosystem.config.js << EOL
module.exports = {
  apps: [{
    name: 'hahd-server',
    script: 'server.js',
    cwd: '/opt/hahd/server',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '500M',
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
EOL

# Configure Nginx
echo "Configuring Nginx..."
cat > /etc/nginx/nginx.conf << 'NGINXEOF'
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
        
        location /api/ {
            proxy_pass http://nodejs_backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
        
        location /auth/ {
            proxy_pass http://nodejs_backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
        
        location /survey/ {
            proxy_pass http://nodejs_backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
        
        location /health {
            proxy_pass http://nodejs_backend;
            access_log off;
        }
        
        location / {
            return 200 'HAHD Server is running on EC2! Node.js backend is proxied at /api, /auth, /survey endpoints.';
            add_header Content-Type text/plain;
        }
    }
}
NGINXEOF

echo "Starting Nginx..."
systemctl enable nginx
systemctl start nginx

# Start Node.js application
echo "Starting Node.js application..."
if [ -f "/opt/hahd/server/server.js" ]; then
    sudo -u ec2-user bash -c '
        cd /opt/hahd
        export PATH=$PATH:/usr/bin:/usr/local/bin
        pm2 start ecosystem.config.js
        pm2 save
    '
    
    # Setup PM2 to start on boot
    env PATH=$PATH:/usr/bin /usr/lib/node_modules/pm2/bin/pm2 startup systemd -u ec2-user --hp /home/ec2-user
    
    echo "Application started with PM2"
else
    echo "server.js not found, creating a test server..."
    cat > /opt/hahd/server/test-server.js << 'TESTJS'
const express = require('express');
const app = express();
const port = 3001;

app.use(express.json());

app.get('/health', (req, res) => {
    res.json({ status: 'OK', message: 'HAHD Test Server is running', timestamp: new Date().toISOString() });
});

app.get('/api/videos/test-s3', (req, res) => {
    res.json({ message: 'S3 connection would be tested here', status: 'mock' });
});

app.post('/auth/signIn', (req, res) => {
    res.json({ message: 'Mock sign in', status: 'success' });
});

app.listen(port, () => {
    console.log(`HAHD test server running on port ${port}`);
});
TESTJS
    
    chown ec2-user:ec2-user /opt/hahd/server/test-server.js
    
    # Update PM2 config to use test server
    cat > /opt/hahd/ecosystem.config.js << EOL
module.exports = {
  apps: [{
    name: 'hahd-server',
    script: 'test-server.js',
    cwd: '/opt/hahd/server',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '500M',
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
EOL
    
    # Install express for test server
    cd /opt/hahd/server
    npm init -y
    npm install express
    
    sudo -u ec2-user bash -c '
        cd /opt/hahd
        export PATH=$PATH:/usr/bin:/usr/local/bin
        pm2 start ecosystem.config.js
        pm2 save
    '
    
    echo "Test server started"
fi

echo "HAHD server setup completed at $(date)"
echo "Server should be available at http://$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)"