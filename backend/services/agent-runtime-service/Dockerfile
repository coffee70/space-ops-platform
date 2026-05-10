FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json tsconfig.json ./
RUN npm ci

COPY src ./src
COPY config ./config

RUN npm run build

EXPOSE 8080

CMD ["node", "dist/server.js"]
