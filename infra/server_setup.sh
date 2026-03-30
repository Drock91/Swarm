#!/bin/bash
# server_setup.sh — Run once on a fresh Amazon Linux 2023 instance
# Usage: bash server_setup.sh

set -e

echo "==> Updating system..."
sudo dnf update -y

echo "==> Installing Node.js 20..."
sudo dnf install -y nodejs npm

echo "==> Installing git, nginx..."
sudo dnf install -y git nginx

echo "==> Installing PM2..."
sudo npm install -g pm2

echo "==> Creating app directory..."
mkdir -p /home/ec2-user/swarm
cd /home/ec2-user/swarm

echo "==> Installing dependencies..."
npm install --omit=dev

echo "==> Configuring nginx..."
sudo tee /etc/nginx/conf.d/swarm.conf > /dev/null << 'NGINX'
server {
    listen 80;
    server_name webhook.heinrichstech.com;

    location / {
        proxy_pass         http://127.0.0.1:3001;
        proxy_http_version 1.1;
        proxy_set_header   Host              $host;
        proxy_set_header   X-Real-IP         $remote_addr;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
        proxy_read_timeout 30s;
    }
}
NGINX

sudo nginx -t
sudo systemctl enable nginx
sudo systemctl restart nginx

echo "==> Starting swarm processes with PM2..."
cd /home/ec2-user/swarm
pm2 start ecosystem.config.cjs
pm2 save

echo "==> Configuring PM2 to start on boot..."
sudo env PATH=$PATH:/usr/bin pm2 startup systemd -u ec2-user --hp /home/ec2-user
pm2 save

echo ""
echo "========================================="
echo "  Setup complete."
echo "  webhook_server : http://webhook.heinrichstech.com/health"
echo "  Logs           : pm2 logs"
echo "  Status         : pm2 status"
echo "========================================="
