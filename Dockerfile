FROM ubuntu:latest
LABEL authors="kael"

ENTRYPOINT ["top", "-b"]