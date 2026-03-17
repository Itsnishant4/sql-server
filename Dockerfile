FROM node:20-slim
WORKDIR /app

# Install pnpm
RUN npm install -g pnpm

# Copy package files
COPY package.json pnpm-lock.yaml* ./
RUN pnpm install --prod

# Copy application code
COPY . .

# Setup database and uploads directory
RUN mkdir -p databases uploads

EXPOSE 8080

CMD ["node", "index.js"]
