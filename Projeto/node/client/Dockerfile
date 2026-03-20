FROM node:20-slim
WORKDIR /app
COPY package.json .
RUN npm install --omit=dev
COPY client.js .
CMD ["node", "client.js"]
