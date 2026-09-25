FROM node:22.22.0-bookworm-slim@sha256:dd9d21971ec4395903fa6143c2b9267d048ae01ca6d3ea96f16cb30df6187d94
WORKDIR /app/packages/node
COPY packages/node/package*.json ./
RUN npm ci
COPY packages/node/tsconfig.json ./
COPY packages/node/src ./src
COPY packages/node/test ./test
COPY tests/contracts /app/tests/contracts
RUN npm run build && npm run lint
CMD ["npm", "test"]
