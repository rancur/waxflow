FROM node:20-alpine AS builder
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci
COPY . .
# Belt and braces: public/ is tracked (see public/.gitkeep), but the COPY in the
# runner stage hard-fails if it is ever missing, which is how fresh-clone builds
# broke. Guarantee it exists rather than depending on the build context.
RUN mkdir -p /app/public

# NOTE: the frontend calls a RELATIVE /api and next.config.js rewrites it
# server-side using INTERNAL_API_URL at RUNTIME, so nothing user-specific is
# baked in here. That is what makes one published image work for every install.
ARG NEXT_PUBLIC_API_URL=http://localhost:8402
ARG INTERNAL_API_URL=http://sync-api:8402
ENV NEXT_PUBLIC_API_URL=$NEXT_PUBLIC_API_URL
ENV INTERNAL_API_URL=$INTERNAL_API_URL
RUN npm run build

FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public
EXPOSE 3000
CMD ["node", "server.js"]
