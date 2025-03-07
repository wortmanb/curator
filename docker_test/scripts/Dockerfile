# syntax=docker/dockerfile:experimental
ARG VERSION=8.17.2
FROM elasticsearch:${VERSION}

ENV ES_JAVA_OPTS=-XX:UseSVE=0
ENV CLI_JAVA_OPTS=-XX:UseSVE=0

COPY --chown=1000:0 small.options /usr/share/elasticsearch/config/jvm.options.d
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["eswrapper"]
