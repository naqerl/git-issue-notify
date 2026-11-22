# syntax=docker/dockerfile:1

FROM golang:1.26-bookworm AS build

WORKDIR /src

COPY go.mod ./
COPY *.go ./

ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/git-issue-notify .

# Distroless static: no shell, no libc; the binary is pure Go and only needs
# CA roots for an HTTPS ntfy or Gitea URL.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/git-issue-notify /usr/local/bin/git-issue-notify

USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/git-issue-notify"]
