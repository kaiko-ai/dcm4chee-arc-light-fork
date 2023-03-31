ARG ARCHIVE_VERSION=latest
FROM dcm4che/dcm4chee-arc-psql:${ARCHIVE_VERSION}

ARG DEPLOYMENT_DIR=/docker-entrypoint.d/deployments/

# COPY dcm4chee-arc-ui2-${ARCHIVE_VERSION}-secure.war $DEPLOYMENT_DIR
COPY dcm4chee-arc-ear/target/dcm4chee-arc-ear-${ARCHIVE_VERSION}-psql-secure.ear $DEPLOYMENT_DIR
RUN cd $DEPLOYMENT_DIR && chown wildfly:wildfly *
