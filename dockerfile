# Stage 1: Build the tracker
FROM golang:1.27.1-alpine AS tracker-build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /out/tracker ./cmd/tracker


# Stage 2: Build the client
FROM golang:1.27.1-alpine AS client-build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /out/client ./cmd/client


# Stage 3: Run the tracker
FROM alpine:3.22 AS tracker

COPY --from=tracker-build /out/tracker /app/tracker

EXPOSE 8080

ENTRYPOINT ["/app/tracker"]


# Stage 4: Run the client
FROM alpine:3.22 AS client

COPY --from=client-build /out/client /app/client

ENTRYPOINT ["/app/client"]
