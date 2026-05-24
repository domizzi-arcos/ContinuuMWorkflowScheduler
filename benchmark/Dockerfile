# Multi-stage build for the benchmark grid runner.
# Build context is the project root (so we can copy go.mod and internal/);
# invoke via: docker build -f benchmark/Dockerfile .

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal/ ./internal/
COPY benchmark/main.go ./benchmark/main.go
RUN CGO_ENABLED=0 go build -o /out/benchmark ./benchmark

FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/benchmark /app/benchmark
ENTRYPOINT ["/app/benchmark"]
