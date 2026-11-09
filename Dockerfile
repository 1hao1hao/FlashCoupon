FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /flashcoupon ./cmd/server
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /flashcoupon /flashcoupon
EXPOSE 8080
ENTRYPOINT ["/flashcoupon"]
