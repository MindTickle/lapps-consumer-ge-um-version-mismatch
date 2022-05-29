FROM golang:1.18.2 as builder

RUN apt-get update && apt-get install -y ca-certificates git-core ssh

ADD ./go.mod /app/go.mod
ADD ./go.sum /app/go.sum

ADD ./healthCheck.sh /healthCheck.sh
ADD healthCheck/healthCheck.go /healthCheck.go

#adding ssh keys
ADD id_rsa /root/.ssh/id_rsa
RUN chmod 700 /root/.ssh/id_rsa

#setting git configs
RUN echo "Host github.com\n\tStrictHostKeyChecking no\n" >> /root/.ssh/config
RUN git config --global url."git@github.com:".insteadOf "https://github.com/"


#exporting go1.11 module support variable
ENV GO111MODULE=on

WORKDIR /app/

#create vendor directory
RUN go mod download

ADD . /app/

RUN go mod vendor

WORKDIR /app/src/

#building source code
RUN CGO_ENABLED=0 go build -mod=vendor -o main .
RUN CGO_ENABLED=0 go build -mod=vendor -o healthCheck /app/healthCheck

#removing ssh keys
RUN rm -f /root/.ssh/id_rsa
RUN rm -f /app/id_rsa

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app/
RUN mkdir /app/src/config -p
COPY --from=builder /app/src/main .
COPY --from=builder /app/src/config/config.yaml /app/src/config/.
COPY --from=builder /app/src/healthCheck /healthCheck
COPY --from=builder /healthCheck.sh /healthCheck.sh

EXPOSE 80

#adding docker entrypoint
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
CMD ["/docker-entrypoint.sh"]