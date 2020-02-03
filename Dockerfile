# THIS FILE IS PART OF THE CYLC SUITE ENGINE.
# Copyright (C) 2008-2019 NIWA & British Crown (Met Office) & Contributors.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

FROM python:3.7.6-alpine3.11

# A docker container to install cylc-flow from the local directory, as
# well as all dependencies.
# Install dependencies with apk and pip

RUN apk update && \
    apk add --no-cache \
    bash \
    libzmq \
    zeromq-dev && \
    apk add --no-cache --virtual build-dependencies \
    gcc \
    git \
    g++ \
    python3-dev
    #apk del build-dependencies

# Add a non-root user

ENV USER_NAME cylc
ENV USER_GROUP cylc
ENV USER_ROOT /home/${USER_NAME}
ENV USER_ID 6000
RUN addgroup -g 1000 cylc && \
    adduser --shell /bin/bash --uid ${USER_ID} -D --home ${USER_ROOT} ${USER_NAME} -G ${USER_GROUP}

WORKDIR ${USER_ROOT}

# Change ownerships...
# and permissions, sudoers etc...
RUN chown -R ${USER_NAME}:${USER_GROUP} ${USER_ROOT} && \
    chown -R ${USER_NAME}:${USER_GROUP} /tmp

# we can now switch to our non-root user...
USER ${USER_ID}
ENV HOME ${USER_ROOT}
ENV PYTHONHTTPSVERIFY 0
ENV PYTHONWARNINGS ignore

RUN mkdir -p ${USER_ROOT}/cylc-flow && \
    chown -R ${USER_NAME}:${USER_GROUP} ${USER_ROOT}/cylc-flow

VOLUME ${USER_ROOT}/cylc-flow

WORKDIR ${USER_ROOT}/cylc-flow

ENTRYPOINT ["tail", "-f", "/dev/null"]
