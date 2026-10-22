FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod main.go ./
COPY network ./network
COPY server ./server
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/egress-proxy .

FROM alpine:3.22

RUN addgroup -S -g 10001 egress \
    && adduser -S -D -H -u 10001 -G egress egress
COPY --from=build /out/egress-proxy /usr/local/bin/egress-proxy

USER 10001:10001
EXPOSE 18084
HEALTHCHECK --interval=2s --timeout=3s --start-period=2s --retries=3 \
    CMD ["/bin/busybox", "wget", "-Y", "off", "-q", "-T", "2", "-O", "/dev/null", "http://127.0.0.1:18084/healthz"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/egress-proxy", "-listen", "0.0.0.0:18084"]
CMD []
