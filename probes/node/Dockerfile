ARG NODE_IMAGE=node:22.22.0-bookworm-slim@sha256:dd9d21971ec4395903fa6143c2b9267d048ae01ca6d3ea96f16cb30df6187d94
FROM ${NODE_IMAGE} AS build
WORKDIR /app
COPY probes/node/package*.json ./
RUN npm ci
COPY probes/node/tsconfig.json probes/node/vitest.config.ts ./
COPY probes/node/src ./src
COPY probes/node/test ./test
COPY tests/fixtures /tests/fixtures
RUN npm run build && npm run lint
FROM build AS test
ENV CONTRACT_FIXTURES=/tests/fixtures/contracts.json
CMD ["npm","test"]
FROM build AS runtime
RUN npm prune --omit=dev
USER node
EXPOSE 8000
CMD ["node","dist/main.js"]


