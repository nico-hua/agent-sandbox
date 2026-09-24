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
ENTRYPOINT ["/usr/local/bin/agent", "-listen", "0.0.0.0:8080"]
