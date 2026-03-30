module.exports = {
  apps: [
    {
      name:         'webhook',
      script:       'webhook_server.mjs',
      interpreter:  'node',
      cwd:          '/home/ec2-user/swarm',
      instances:    1,
      autorestart:  true,
      watch:        false,
      max_memory_restart: '256M',
      env: { NODE_ENV: 'production' },
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
    },
    {
      name:         'email-node',
      script:       'run.mjs',
      interpreter:  'node',
      args:         'email_node',
      cwd:          '/home/ec2-user/swarm',
      instances:    1,
      autorestart:  true,
      watch:        false,
      max_memory_restart: '256M',
      env: { NODE_ENV: 'production' },
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
    },
  ],
};
