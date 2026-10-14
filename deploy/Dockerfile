# One small image for every part of the system; Compose picks the part with the command.
FROM golang:1.26.9-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Static binary: runs on a distroless base with no libc.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ledgerline ./cmd/ledgerline

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ledgerline /ledgerline
ENTRYPOINT ["/ledgerline"]
