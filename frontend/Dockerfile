# ─── Base ─────────────────────────────────────────────────────────────────────
FROM node:20-alpine AS base
WORKDIR /app
ENV NEXT_TELEMETRY_DISABLED=1

# ─── Development ──────────────────────────────────────────────────────────────
FROM base AS development
COPY package*.json ./
RUN npm install
COPY . .
EXPOSE 3000
CMD ["npm", "run", "dev"]

# ─── Builder ──────────────────────────────────────────────────────────────────
FROM base AS builder
# Recibir variables para el build de Next.js
ARG NEXT_PUBLIC_API_URL
ENV NEXT_PUBLIC_API_URL=$NEXT_PUBLIC_API_URL

COPY package*.json ./
RUN npm ci
COPY . .
# Asegurar permisos de ejecución para binarios de node_modules
RUN chmod -R +x node_modules/.bin
RUN npm run build

# ─── Production ───────────────────────────────────────────────────────────────
FROM base AS production
ENV NODE_ENV=production

# Copy only the standalone output and static assets
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

EXPOSE 3000
CMD ["node", "server.js"]
