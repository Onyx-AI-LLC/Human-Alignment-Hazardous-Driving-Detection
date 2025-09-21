#!/bin/bash
yum update -y
yum install -y httpd
echo "<h1>HAHD Test Server is Working!</h1><p>Instance ID: $(curl -s http://169.254.169.254/latest/meta-data/instance-id)</p><p>Time: $(date)</p>" > /var/www/html/index.html
systemctl start httpd
systemctl enable httpd