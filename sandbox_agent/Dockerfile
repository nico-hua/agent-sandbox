ARG RUNTIME_BASE_IMAGE=alpine:3.22

FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY main.go ./
COPY command ./command
COPY server ./server
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/agent .

FROM ${RUNTIME_BASE_IMAGE}

# Base images can select their own user; build a dedicated runtime account.
USER root
RUN addgroup -S sandbox \
    && adduser -S -G sandbox sandbox \
    && mkdir -p /workspace \
    && chown sandbox:sandbox /workspace

COPY --from=build /out/agent /usr/local/bin/agent

USER sandbox
WORKDIR /workspace
EXPOSE 8080
HEALTHCHECK --interval=2s --timeout=3s --start-period=5s --retries=3 \
    CMD ["wget", "-q", "-T", "2", "-O", "/dev/null", "http://127.0.0.1:8080/healthz"]
ENTRYPOINT ["/usr/local/bin/agent", "-listen", "0.0.0.0:8080"]
# Clear inherited language CMD arguments as well as overriding its entrypoint.
CMD []
