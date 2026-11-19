FROM node:24-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY server.ts ./
COPY portal ./portal
ENV NODE_ENV=production
EXPOSE 8080
CMD ["node", "server.ts"]
