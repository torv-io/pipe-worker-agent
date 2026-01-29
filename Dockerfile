# Build stage
FROM golang:1.24-alpine AS builder

# Install protoc compiler and required tools
RUN apk add --no-cache \
    protobuf \
    protobuf-dev \
    git \
    make

# Install protoc plugins for Go
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Copy proto files
COPY proto/ ./proto/

# Copy source code (needed for go mod to resolve imports)
COPY main.go ./

# Generate proto code
RUN protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/worker.proto

# Update go.sum with all dependencies
RUN go mod tidy

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o worker-agent .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /build/worker-agent .

# Expose gRPC port
EXPOSE 50051

# Run the binary
CMD ["./worker-agent"]
