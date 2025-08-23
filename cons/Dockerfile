FROM golang:1.24.3-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

COPY .env .env

RUN go build -o consumer ./cons

CMD ["./consumer"]