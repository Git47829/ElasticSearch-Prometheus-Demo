FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/logger-client .

FROM alpine:3
COPY --from=build /out/logger-client /usr/local/bin/logger-client
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/logger-client"]
