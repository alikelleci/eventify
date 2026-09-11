FROM node:22-alpine AS build
WORKDIR /app
COPY src/main/frontend/eventify-console-ui/package*.json ./
RUN npm ci
COPY src/main/frontend/eventify-console-ui/ ./
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist/eventify-console-ui/browser /usr/share/nginx/html/console
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY entrypoint.sh /entrypoint.sh
RUN apk add --no-cache yq && chmod +x /entrypoint.sh

EXPOSE 80
ENTRYPOINT ["/entrypoint.sh"]
