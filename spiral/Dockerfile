# The Synapse image Spiral's homeserver runs, v1.158.0-spiral.4: Element's
# official image of the same Synapse version, with this branch's Python
# package in place of its own. The Rust extension module is unchanged, so
# it is taken from the official image as it is.
#
# From the repository root:
#   docker build -f spiral/Dockerfile -t synapse:v1.158.0-spiral.4 .

ARG SYNAPSE_VERSION=v1.158.0
FROM matrixdotorg/synapse:${SYNAPSE_VERSION}
COPY synapse/ /usr/local/lib/python3.13/site-packages/synapse/
# The patched modules must import.
RUN python -c "import synapse.rest.client.sync, synapse.rest.client.presence, synapse.handlers.presence, synapse.rest.client.versions, synapse.handlers.appservice, synapse.storage.databases.main.deviceinbox"
