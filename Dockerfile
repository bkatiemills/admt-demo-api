FROM rust:1.79.0

RUN apt-get update -y ; apt-get install -y nano netcdf-bin libhdf5-serial-dev libnetcdff-dev ; apt-get upgrade -y

COPY api /app
WORKDIR /app
RUN cargo build --release
RUN chown -R 1000660000 /app
CMD ["/app/target/release/admt_api"]
