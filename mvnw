#!/bin/sh
# Maven wrapper script
# Usage: ./mvnw [goals]

set -e

MAVEN_WRAPPER_JAR=".mvn/wrapper/maven-wrapper.jar"
MAVEN_WRAPPER_PROPERTIES=".mvn/wrapper/maven-wrapper.properties"

# Read distribution URL from properties
DISTRIBUTION_URL=$(grep 'distributionUrl' "$MAVEN_WRAPPER_PROPERTIES" | cut -d'=' -f2)

# Resolve MAVEN_HOME
if [ -z "$MAVEN_HOME" ]; then
  MAVEN_HOME="$HOME/.m2/wrapper/dists/apache-maven-3.9.6"
fi

MVN_CMD="$MAVEN_HOME/bin/mvn"

# If Maven is already installed and on PATH, use it
if command -v mvn > /dev/null 2>&1; then
  exec mvn "$@"
fi

echo "Please install Maven or ensure it is on your PATH."
echo "Download: $DISTRIBUTION_URL"
exit 1
