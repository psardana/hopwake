# Builds one Go command from the reference pipeline: --build-arg CMD=bridge
# CGO=1 builds with cgo, for commands that use the C device hook (protosim).
FROM golang:1.27-alpine AS build
ARG CMD
ARG CGO=0
RUN if [ "$CGO" = 1 ]; then apk add --no-cache build-base; fi
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=${CGO} go build -o /out/app ./reference/${CMD}

FROM alpine:3.22
COPY --from=build /out/app /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]
