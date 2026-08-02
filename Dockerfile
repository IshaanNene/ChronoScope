# Distroless: the final image carries the binary and nothing else — no shell,
# no package manager, no libc surface to audit. A consensus node's blast radius
# is the whole cluster's data, so its attack surface is worth minimising.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
RUN cargo build --release -p chronolog-server

FROM gcr.io/distroless/cc-debian12
COPY --from=build /src/target/release/chronolog-server /chronolog-server
# 7400 Raft (UDP), 7401 admin (HTTP)
EXPOSE 7400/udp 7401/tcp
VOLUME ["/var/lib/chronolog"]
USER 65532:65532
ENTRYPOINT ["/chronolog-server"]
