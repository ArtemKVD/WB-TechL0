FROM golang:1.24.3-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cons/ ./cons/
COPY cache/ ./cache/
COPY models/ ./models/
COPY storage/ ./storage/

RUN go build -o consumer ./cons

CMD ["./consumer"]