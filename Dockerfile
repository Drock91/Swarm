# ── build stage ───────────────────────────────────────────────────────────────
FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# ── runtime stage ─────────────────────────────────────────────────────────────
FROM node:20-alpine
WORKDIR /app

# Copy deps and source
COPY --from=deps /app/node_modules ./node_modules
COPY core/     ./core/
COPY nodes/    ./nodes/
COPY run.mjs   ./run.mjs
COPY swarm.mjs ./swarm.mjs

# NODE_TYPE is set per-container in docker-compose.yml (or ECS task def)
ENV NODE_ENV=production

# Tini gives proper signal handling (PID 1 issue)
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]

CMD ["node", "run.mjs"]
