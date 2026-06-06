ARG APP_NAME=mopidy-sound-visualizer

FROM rust:1.95 AS build
ARG APP_NAME
WORKDIR /app

# Install gstreamer (gstreamer crate dependency).
RUN apt-get update && apt-get install -y --no-install-recommends libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
      gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
      gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly \
      gstreamer1.0-libav libgstrtspserver-1.0-dev libges-1.0-dev

# Build the application.
RUN --mount=type=bind,source=src,target=src \
    --mount=type=bind,source=.cargo,target=.cargo \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock \
    --mount=type=cache,target=/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    cargo build --locked --release && \
    cp ./target/release/$APP_NAME /bin/server

# Gstreamer from host.
EXPOSE 5556/udp
# SSE from container.
EXPOSE 3000

CMD ["/bin/server"]
