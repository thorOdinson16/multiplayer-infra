# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o game-room-server ./cmd/main.go

# Run stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /app/game-room-server .

RUN adduser -D -g '' appuser
USER appuser

EXPOSE 8080 7000

CMD ["./game-room-server"]