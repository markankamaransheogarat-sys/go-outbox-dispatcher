FROM golang:1.26-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /outbox-dispatcher ./cmd/dispatcher

FROM gcr.io/distroless/static-debian12:nonroot
USER nonroot:nonroot
COPY --from=builder /outbox-dispatcher /outbox-dispatcher

EXPOSE 9091
ENTRYPOINT ["/outbox-dispatcher"]
