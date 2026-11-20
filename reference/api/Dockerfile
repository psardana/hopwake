FROM node:24-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY server.ts ./
COPY portal ./portal
ENV NODE_ENV=production
EXPOSE 8080
# Stock OpenTelemetry auto-instrumentation (HTTP and Postgres), configured by
# OTEL_* variables; OTEL_SDK_DISABLED=true turns it off.
CMD ["node", "--import", "@opentelemetry/auto-instrumentations-node/register", "server.ts"]
