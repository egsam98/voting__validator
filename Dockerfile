FROM golang:1.16-alpine as builder

RUN apk update && \
    apk add --no-cache git

WORKDIR /validator

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o bin/validator *.go

FROM scratch
COPY --from=builder /validator/bin/validator .
ENTRYPOINT ["./validator"]
