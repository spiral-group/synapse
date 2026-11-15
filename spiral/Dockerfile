# The Synapse image Spiral's homeserver runs, v1.158.0-spiral.8: Element's
# official image of the same Synapse version, with this branch's Python
# package in place of its own. The Rust extension module is unchanged, so
# it is taken from the official image as it is.
#
# From the repository root:
#   docker build -f spiral/Dockerfile -t synapse:v1.158.0-spiral.8 .

ARG SYNAPSE_VERSION=v1.158.0
FROM matrixdotorg/synapse:${SYNAPSE_VERSION}
# Two dependencies newer than the base image's lock: Pillow 13.0.0 (Synapse
# decodes and re-encodes every uploaded image for its thumbnails; 12.3.0 has
# known advisories, a JPEG decoder overflow among them) and msgpack 1.2.4
# (GHSA-j586-36cw-2gc2). Upstream ships the same pins from 1.163.2 and
# 1.164.1. The build fails unless Python imports them with the codecs the
# base image had.
RUN pip install --no-cache-dir 'pillow==13.0.0' 'msgpack==1.2.4' \
    && python -c "import PIL, msgpack; from PIL import features; assert PIL.__version__ == '13.0.0', PIL.__version__; assert msgpack.version == (1, 2, 4), msgpack.version; missing = [f for f in ('jpg', 'jpg_2000', 'webp', 'avif', 'libtiff', 'zlib') if not features.check(f)]; assert not missing, missing"
COPY synapse/ /usr/local/lib/python3.13/site-packages/synapse/
COPY spiral/modules/spiral_post_guard.py /usr/local/lib/python3.13/site-packages/spiral_post_guard.py
# The patched modules must import.
RUN python -c "import synapse.rest.client.sync, synapse.rest.client.presence, synapse.handlers.presence, synapse.rest.client.versions, synapse.handlers.appservice, synapse.storage.databases.main.deviceinbox"
RUN python -c "import spiral_post_guard"
