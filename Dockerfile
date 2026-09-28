FROM rust:1-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN groupadd --gid 10001 lqs \
    && useradd --uid 10001 --gid lqs --no-create-home --shell /usr/sbin/nologin lqs \
    && mkdir /data \
    && chown lqs:lqs /data

COPY --from=builder /app/target/release/lqs /usr/local/bin/lqs

ENV LQS_BIND_ADDR=0.0.0.0:9324 \
    LQS_BASE_URL=http://127.0.0.1:9324 \
    LQS_DATABASE_PATH=/data/lqs.sqlite

USER lqs
EXPOSE 9324
CMD ["lqs"]
