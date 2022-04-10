FROM rust:1.60 as chef
ENV CARGO_TERM_COLOR=always
WORKDIR /app
RUN cargo install cargo-chef
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools
RUN rustup target add x86_64-unknown-linux-musl
RUN rustup component add clippy

FROM chef as planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --verbose --release --target x86_64-unknown-linux-musl
RUN cargo clippy --release --target x86_64-unknown-linux-musl --no-deps -- --deny "warnings"
RUN cargo test --verbose --release --target x86_64-unknown-linux-musl

FROM scratch AS runtime
USER 1000
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/jarvis-spot-price-exporter .
ENTRYPOINT ["./jarvis-spot-price-exporter"]