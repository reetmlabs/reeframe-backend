#!/bin/bash
# Builds reeframe-backend_VERSION_DISTRO_ARCH.deb in the current directory.
# DISTRO is debian12 or ubuntu24.04 and picks the Depends list; the Ubuntu
# package also needs the gst-libav plugin from dist/ubuntu/build-gst-libav.sh.
set -euo pipefail

usage="usage: build-deb.sh DISTRO ARCH VERSION VMS_DAEMON [LIBGSTLIBAV_SO]"
DISTRO=${1:?$usage}
ARCH=${2:?$usage}
VERSION=${3:?$usage}
BINARY=${4:?$usage}
LIBAV=${5:-}
DIST=$(cd "$(dirname "$0")/.." && pwd)
ROOT=$(cd "$DIST/.." && pwd)
DEPENDS_FILE="$DIST/debian/depends-$DISTRO"

[ -f "$DEPENDS_FILE" ] || { echo "unknown distro: $DISTRO" >&2; exit 1; }
if [ "$DISTRO" = ubuntu24.04 ] && [ -z "$LIBAV" ]; then
    echo "the ubuntu24.04 package needs LIBGSTLIBAV_SO" >&2
    exit 1
fi

NAME="reeframe-backend_${VERSION}_${DISTRO}_${ARCH}"
PKG=$(mktemp -d)
trap 'rm -rf "$PKG"' EXIT
chmod 0755 "$PKG"

install -d "$PKG/DEBIAN"
install -m 0755 "$DIST/debian/postinst" "$DIST/debian/postrm" "$PKG/DEBIAN/"
# Depends is one comma-separated line built from the per-distro list.
depends=$(grep -Ev '^\s*(#|$)' "$DEPENDS_FILE" | paste -sd, - | sed 's/,/, /g')
sed -e "s/^Version:.*/Version: ${VERSION}/" \
    -e "s/^Architecture:.*/Architecture: ${ARCH}/" \
    "$DIST/debian/control" > "$PKG/DEBIAN/control"
echo "Depends: $depends" >> "$PKG/DEBIAN/control"

install -D -m 0755 "$BINARY" "$PKG/usr/bin/vms-daemon"
install -D -m 0644 "$DIST/reeframe.service" "$PKG/usr/lib/systemd/system/reeframe.service"
install -D -m 0644 "$ROOT/LICENSE" "$PKG/usr/share/doc/reeframe-backend/LICENSE"
install -D -m 0644 "$ROOT/NOTICE" "$PKG/usr/share/doc/reeframe-backend/NOTICE"
if [ -n "$LIBAV" ]; then
    install -D -m 0644 "$LIBAV" "$PKG/usr/lib/reeframe/gstreamer-1.0/libgstlibav.so"
fi

dpkg-deb --build --root-owner-group "$PKG" "$NAME.deb" >/dev/null
echo "$NAME.deb"
