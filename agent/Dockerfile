FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY main.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/agent .

FROM alpine:3.22

RUN addgroup -S sandbox \
    && adduser -S -G sandbox sandbox \
    && mkdir /workspace \
    && chown sandbox:sandbox /workspace

COPY --from=build /out/agent /usr/local/bin/agent

USER sandbox
WORKDIR /workspace
EXPOSE 8080
HEALTHCHECK --interval=2s --timeout=3s --start-period=5s --retries=3 \
    CMD ["wget", "-q", "-T", "2", "-O", "/dev/null", "http://127.0.0.1:8080/healthz"]
ENTRYPOINT ["/usr/local/bin/agent", "-listen", "0.0.0.0:8080"]
