# Dockerfile which uses multistage build and `cargo-chef`
# https://github.com/LukeMathWalker/cargo-chef to cache cargo dependencies.
# Uses https://github.com/emk/rust-musl-builder base image for building
# a static binary which can be deployed to an alpine linux image.

FROM ekidd/rust-musl-builder as PLANNER

RUN cargo install cargo-chef --version 0.1.19
ADD --chown=rust:rust . ./
RUN cargo chef prepare --recipe-path recipe.json

FROM ekidd/rust-musl-builder as CACHER

RUN cargo install cargo-chef --version 0.1.19
COPY --from=PLANNER /home/rust/src/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path /home/rust/src/recipe.json

FROM ekidd/rust-musl-builder:latest AS BUILDER

ADD --chown=rust:rust . ./
COPY --from=CACHER /home/rust/src/target target
COPY --from=CACHER $CARGO_HOME $CARGO_HOME
RUN cargo build --release

FROM alpine:latest

RUN apk --no-cache add ca-certificates
COPY --from=BUILDER \
            /home/rust/src/target/x86_64-unknown-linux-musl/release/aquadoggo \
            /usr/local/bin/
CMD /usr/local/bin/aquadoggo
