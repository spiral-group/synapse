# The Synapse image Spiral's homeserver runs, v1.165.0-spiral.1: Element's
# official image of the same Synapse version, with this branch's Python
# package in place of its own. The Rust extension module is unchanged, so
# it is taken from the official image as it is.
#
# From the repository root:
#   docker build -f spiral/Dockerfile -t synapse:v1.165.0-spiral.1 .

ARG SYNAPSE_VERSION=v1.165.0
FROM matrixdotorg/synapse:${SYNAPSE_VERSION}
# Pillow 13.0.0 and msgpack 1.2.4 (spiral.8 pinned them on 1.158) are in
# 1.165's own lock; the build fails if the base image ever has older ones,
# or Pillow without the codecs it had.
RUN python -c "import PIL, msgpack; from PIL import features; assert tuple(map(int, PIL.__version__.split('.')[:3])) >= (13, 0, 0), PIL.__version__; assert msgpack.version >= (1, 2, 4), msgpack.version; missing = [f for f in ('jpg', 'jpg_2000', 'webp', 'avif', 'libtiff', 'zlib') if not features.check(f)]; assert not missing, missing"
COPY synapse/ /usr/local/lib/python3.13/site-packages/synapse/
COPY spiral/modules/spiral_post_guard.py /usr/local/lib/python3.13/site-packages/spiral_post_guard.py
# The patched modules must import.
RUN python -c "import synapse.rest.client.sync, synapse.rest.client.presence, synapse.handlers.presence, synapse.handlers.appservice, synapse.storage.databases.main.deviceinbox, synapse.appservice.scheduler"
RUN python -c "import spiral_post_guard"
