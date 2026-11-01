#!/bin/sh
set -eu

PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
JAR="$PROJECT_DIR/target/aeron-failover-1.0.0.jar"
CONFIG_FILE=${AERON_DEMO_CONFIG:-"$PROJECT_DIR/config.properties"}

if [ ! -f "$JAR" ]; then
    echo "Missing $JAR; build the project first with mvn package." >&2
    exit 1
fi
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Missing $CONFIG_FILE; copy config.properties.example to config.properties and edit it." >&2
    exit 1
fi

exec java --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
    -Daeron.demo.config="$CONFIG_FILE" \
    -cp "$JAR" "$@"
