# EC2 Migration Guide: Render to AWS EC2

This guide walks you through migrating your Human Alignment Hazardous Driving Detection server from Render to AWS EC2, with S3 backups for MongoDB data.

## 🎯 **Migration Overview**

**What's happening:**
- **Server**: Moving from Render → AWS EC2 (cost-optimized)
- **Frontend**: Stays on HostGator (just API URL updates needed)
- **Database**: Keeps MongoDB Atlas + adds S3 backups
- **Videos**: Still uses existing S3 bucket
- **Monitoring**: Adds CloudWatch + automated backups

**Estimated Cost:** ~$10-15/month (vs current Render cost)

## 🏗️ **Architecture**

```
Frontend (HostGator) → Load Balancer → EC2 Instance → MongoDB Atlas
                                     ↓
                                S3 Backups (Daily)
                                     ↓
                              CloudWatch Monitoring
```

**EC2 Instance Specs:**
- **Instance Type**: t3.micro (1 vCPU, 1GB RAM) - can upgrade to t3.small if needed
- **Storage**: 20GB EBS (expandable)
- **OS**: Amazon Linux 2023
- **Services**: Node.js 18, Nginx, PM2, CloudWatch agent

## 📋 **Prerequisites**

1. **AWS Account** with CLI configured
2. **EC2 Key Pair** for SSH access
3. **Domain name** (optional, can use IP address)
4. **HostGator access** for frontend updates

## 🚀 **Deployment Steps**

### Step 1: Deploy EC2 Infrastructure

```bash
cd ec2-migration
./deploy-ec2.sh your-key-pair-name [optional-domain-name] [optional-jwt-secret]
```

**Example:**
```bash
./deploy-ec2.sh my-aws-keypair hahd.myapp.com
```

This will:
- ✅ Create EC2 instance with auto-scaling group
- ✅ Set up S3 backup bucket
- ✅ Configure CloudWatch monitoring
- ✅ Install Node.js, Nginx, PM2
- ✅ Deploy your application code
- ✅ Set up daily MongoDB backups

### Step 2: Update Frontend API URLs

After deployment completes, update your frontend:

```bash
cd ec2-migration/frontend-updates
./update-api-urls.js http://YOUR-EC2-IP:3001
```

This will:
- ✅ Replace all hardcoded Render URLs
- ✅ Create environment-based configuration
- ✅ Generate build scripts for production

### Step 3: Deploy Updated Frontend

1. **Update production URL** in `.env.production`:
   ```
   REACT_APP_API_URL=http://your-ec2-ip:3001
   ```

2. **Build for production:**
   ```bash
   npm run build:prod
   ```

3. **Upload to HostGator** (replace contents of public_html or www folder)

### Step 4: Test Everything

- ✅ User registration/login
- ✅ Video playback
- ✅ Survey submission  
- ✅ Raffle leaderboard
- ✅ MongoDB backups

## 📊 **What You Get**

### Server Features
- **Auto-restart**: PM2 manages your Node.js app
- **Load balancing**: Nginx reverse proxy
- **SSL ready**: Easy Let's Encrypt integration
- **Monitoring**: CloudWatch metrics and logs
- **Backups**: Daily MongoDB backups to S3

### Cost Breakdown (Monthly)
- **EC2 t3.micro**: ~$8.50
- **EBS 20GB**: ~$2.00
- **Elastic IP**: ~$3.60 (when instance is stopped)
- **S3 storage**: ~$0.50 (backups)
- **CloudWatch**: ~$1.00 (logs/metrics)
- **Total**: ~$12-15/month

### Monitoring & Alerts
- **CloudWatch Dashboards**: CPU, memory, disk usage
- **Log Aggregation**: Application, Nginx, system logs  
- **Automated Alerts**: High CPU, status check failures
- **Backup Reports**: Daily backup status notifications

## 🔧 **Management Commands**

### Server Management
```bash
# SSH to server
ssh -i ~/.ssh/your-keypair.pem ec2-user@YOUR-EC2-IP

# Check application status
pm2 status

# View logs
tail -f /opt/hahd/logs/combined.log

# Restart application
pm2 restart hahd-server

# Check nginx status
sudo systemctl status nginx
```

### Backup Management
```bash
# Manual backup
cd /opt/hahd && node scripts/mongodb-backup.js backup

# Check backup status
node scripts/mongodb-backup.js health

# List S3 backups
aws s3 ls s3://your-backup-bucket/mongodb-backups/
```

### Monitoring
```bash
# View CloudWatch logs
aws logs tail /aws/hahd/application --follow

# Check metrics
aws cloudwatch get-metric-statistics --namespace AWS/EC2 --metric-name CPUUtilization --start-time 2025-01-19T00:00:00Z --end-time 2025-01-19T23:59:59Z --period 3600 --statistics Average --dimensions Name=InstanceId,Value=i-1234567890abcdef0
```

## 🔄 **Rollback Plan**

If something goes wrong, you can easily rollback:

### Option 1: Quick Frontend Rollback
Update `.env.production` to use Render:
```
REACT_APP_API_URL=https://human-alignment-hazardous-driving.onrender.com
```

Rebuild and redeploy frontend.

### Option 2: Full Rollback
1. Keep EC2 running but unused
2. Update frontend to use Render URLs
3. Your Render server is still running unchanged
4. Delete EC2 stack later when confident

## 🛡️ **Security Features**

- **Security Groups**: Only necessary ports open (22, 80, 443, 3001)
- **IAM Roles**: Least privilege access to AWS services
- **Environment Variables**: Secrets managed securely
- **Automated Patching**: Amazon Linux 2023 auto-updates
- **Network Isolation**: VPC with private subnets option

## 📈 **Scaling Options**

### Immediate Upgrades
- **t3.small** (2 vCPU, 2GB RAM): +$8/month
- **t3.medium** (2 vCPU, 4GB RAM): +$16/month
- **More storage**: EBS scales instantly

### Advanced Scaling
- **Application Load Balancer**: Multiple instances
- **Auto Scaling Groups**: Scale based on demand
- **RDS**: Managed database instead of MongoDB Atlas
- **CloudFront**: CDN for global performance

## 🔍 **Troubleshooting**

### Common Issues

**"Server not responding"**
```bash
# Check if instance is running
aws ec2 describe-instances --instance-ids i-yourinstanceid

# Check application status
ssh -i ~/.ssh/key.pem ec2-user@ip "pm2 status"

# Check nginx
ssh -i ~/.ssh/key.pem ec2-user@ip "sudo systemctl status nginx"
```

**"Frontend can't connect"**
- Check CORS settings in server.js
- Verify security group allows port 3001
- Test with curl: `curl http://your-ip:3001/health`

**"Backup failures"**  
```bash
# Test S3 connectivity
node scripts/mongodb-backup.js test

# Test MongoDB connection
node scripts/mongodb-backup.js health

# Check IAM permissions
aws sts get-caller-identity
```

### Log Locations
- **Application**: `/opt/hahd/logs/combined.log`
- **Nginx Access**: `/var/log/nginx/access.log`
- **Nginx Error**: `/var/log/nginx/error.log`
- **System**: `/var/log/messages`
- **CloudWatch**: AWS Console > CloudWatch > Log Groups

## 📞 **Support**

### Useful Resources
- **AWS EC2 Console**: Monitor instance health
- **CloudWatch Dashboard**: Performance metrics
- **S3 Console**: Check backup files
- **MongoDB Atlas**: Database monitoring

### Performance Optimization
1. **Monitor CloudWatch metrics** for bottlenecks
2. **Upgrade instance size** if CPU/memory is high
3. **Add CloudFront** for faster global access
4. **Optimize database queries** if response times are slow

## 🎉 **Success Checklist**

After migration, verify:

- [ ] EC2 instance running and accessible
- [ ] Node.js application started with PM2
- [ ] Nginx reverse proxy working
- [ ] Frontend connects to EC2 server
- [ ] User registration/login working
- [ ] Video playback functional
- [ ] Survey submission successful
- [ ] MongoDB backups running daily
- [ ] CloudWatch monitoring active
- [ ] All logs being collected

## 📝 **Maintenance Schedule**

### Daily
- ✅ Automated MongoDB backups (2 AM UTC)
- ✅ CloudWatch monitoring alerts

### Weekly  
- 📊 Review performance metrics
- 📁 Check backup success rates
- 💰 Monitor AWS costs

### Monthly
- 🔐 Review security configurations
- 📈 Analyze usage patterns
- 🧹 Clean up old backups (automated)

---

**Ready to migrate?** Run `./deploy-ec2.sh your-keypair` to get started!

**Questions?** Check the troubleshooting section or AWS documentation.