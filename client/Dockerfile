FROM node:20-alpine
WORKDIR /app
COPY package.json ./
COPY public/ ./public/
RUN npm install -g serve
EXPOSE 3000
CMD ["serve", "public", "-l", "3000"]