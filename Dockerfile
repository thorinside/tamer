FROM golang:onbuild

RUN ["apt-get", "update"]
RUN ["apt-get", "install", "-y", "vim"]

ADD dbconf.yml /go/src/app/db/

EXPOSE 8787
