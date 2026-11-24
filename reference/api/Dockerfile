FROM node:24-alpine
WORKDIR /app
# Built from the repository root, to include Hopwake's Node hook.
COPY reference/api/package.json reference/api/package-lock.json ./
RUN npm ci --omit=dev
COPY reference/api/server.ts ./
COPY reference/api/portal ./portal
COPY pkg/hook/node/hopwake-hook.ts ./
ENV NODE_ENV=production
EXPOSE 8080
# Stock OpenTelemetry auto-instrumentation (HTTP and Postgres), configured by
# OTEL_* variables; OTEL_SDK_DISABLED=true turns it off.
CMD ["node", "--import", "@opentelemetry/auto-instrumentations-node/register", "server.ts"]
