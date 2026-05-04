# ── Build stage ────────────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o scheduler ./cmd

# ── Run stage ──────────────────────────────────────────────────────────────────
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/scheduler .
COPY --from=builder /app/config.yaml .

CMD ["./scheduler"]
