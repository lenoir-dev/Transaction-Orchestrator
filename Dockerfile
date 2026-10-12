FROM golang:1.27.2@sha256:5bc7f572bbaa98885a3a1fd9c0aa76b59e3e14e8628bfc316bbfd0c701e4818c AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG SOURCE_IDENTITY=unavailable
ARG RELEASE_GOARCH
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${RELEASE_GOARCH:-$(go env GOARCH)} go build -trimpath -ldflags="-s -w -X main.sourceIdentity=${SOURCE_IDENTITY}" -o /out/txorch ./cmd/txorch

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/txorch /txorch
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/txorch"]
