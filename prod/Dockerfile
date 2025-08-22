FROM golang:1.24.3-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY prod/ ./prod/
COPY models/ ./models/

RUN go build -o producer ./prod

CMD ["./producer"]