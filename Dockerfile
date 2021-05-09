FROM node:8-alpine AS node

ENV NODE_ENV=production

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY package.json yarn.lock /usr/src/app/
RUN yarn

FROM postgres:13-alpine

COPY --from=node /usr/local/bin/node /usr/local/bin/node

COPY --from=node /usr/src/app /usr/src/app
COPY . /usr/src/app
WORKDIR /usr/src/app
RUN set -xe; \
	mv docker/docker-entrypoint-initdb.d/* /docker-entrypoint-initdb.d/; \
	rm -r test docker;
