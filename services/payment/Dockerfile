ARG NODE_IMAGE=node:22.22.0-bookworm-slim@sha256:dd9d21971ec4395903fa6143c2b9267d048ae01ca6d3ea96f16cb30df6187d94
FROM ${NODE_IMAGE} AS build
WORKDIR /app/packages/node
COPY packages/node/package*.json ./
RUN npm ci
COPY packages/node/tsconfig.json ./
COPY packages/node/src ./src
RUN npm run build
WORKDIR /app/services/payment
COPY services/payment/package*.json ./
RUN npm ci
COPY services/payment/tsconfig.json services/payment/schema.sql ./
COPY services/payment/src ./src
RUN npm run build && npm run lint
FROM build AS test
COPY services/payment/test ./test
CMD ["npm", "test"]
FROM build AS runtime
RUN npm prune --omit=dev
USER node
EXPOSE 8000
CMD ["node", "dist/main.js"]
