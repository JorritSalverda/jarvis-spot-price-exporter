FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM debian:bookworm-slim AS runtime

ARG TARGETPLATFORM

WORKDIR /app

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY $TARGETPLATFORM/jarvis .

RUN chmod +x jarvis

ENTRYPOINT ["./jarvis"]