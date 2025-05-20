FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS builder
ARG TARGETOS
ARG TARGETARCH
ENV GOPRIVATE=github.com/decisiveai/mdai-operator
WORKDIR /opt/event-hub-poc

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -mod=vendor -ldflags="-w -s" -o /event-hub-poc .

FROM gcr.io/distroless/static-debian12
WORKDIR /
COPY --from=builder /event-hub-poc /event-hub-poc
CMD ["/event-hub-poc"]
