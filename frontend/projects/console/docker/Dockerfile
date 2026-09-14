FROM node:22-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . ./
RUN npm run build:console

FROM nginx:alpine
COPY --from=build /app/dist/console/browser /usr/share/nginx/html/console
COPY projects/console/docker/nginx.conf /etc/nginx/conf.d/default.conf
COPY projects/console/docker/entrypoint.sh /entrypoint.sh
RUN apk add --no-cache yq && chmod +x /entrypoint.sh

EXPOSE 80
ENTRYPOINT ["/entrypoint.sh"]
