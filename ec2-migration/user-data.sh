#!/bin/bash
yum update -y

# Install Node.js 18 LTS
curl -fsSL https://rpm.nodesource.com/setup_18.x | bash -
yum install -y nodejs git nginx python3 python3-pip

# Install PM2 globally
npm install -g pm2

# Create application directory
mkdir -p /opt/hahd/{server,frontend,data,logs,ssl,scripts}
cd /opt/hahd

# Clone repository
git clone https://github.com/lennox55555/Human-Alignment-Hazardous-Driving-Detection.git temp-repo

# Move server files
cp -r temp-repo/server/* /opt/hahd/server/ 2>/dev/null || true
cp -r temp-repo/frontend/dist/* /opt/hahd/frontend/ 2>/dev/null || true
cp -r temp-repo/ETL /opt/hahd/data/ 2>/dev/null || true
rm -rf temp-repo

# Set proper ownership
chown -R ec2-user:ec2-user /opt/hahd

# Install server dependencies
cd /opt/hahd/server
npm install --production

# Create environment file
cat > /opt/hahd/server/.env << EOL
MONGODB_URI=mongodb+srv://jonahmulcrone:1RFqimbiYxCpSlkU@hahdcluster.igin9.mongodb.net/survey?retryWrites=true&w=majority&appName=HAHDCluster
SECRET=hahd-jwt-secret-$(date +%s)
AWS_REGION=us-east-1
S3_BUCKET_NAME=driving-simulator-videos
PORT=3001
NODE_ENV=production
EOL

# Create PM2 ecosystem file
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
            return 200 'HAHD Server is running on EC2!';
            add_header Content-Type text/plain;
        }
    }
}
NGINXEOF

# Start services
systemctl enable nginx
systemctl start nginx

# Start application as ec2-user
sudo -u ec2-user bash -c '
    cd /opt/hahd
    export PATH=$PATH:/usr/bin
    pm2 start ecosystem.config.js
    pm2 save
'

# Setup PM2 to start on boot
env PATH=$PATH:/usr/bin /usr/lib/node_modules/pm2/bin/pm2 startup systemd -u ec2-user --hp /home/ec2-user