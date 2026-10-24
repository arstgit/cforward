#!/usr/bin/env bash
# Build the cforward Android APK (run from android/).
#   - cross-compiles the Go core (repo root) for android/arm64
#   - fetches cfst + ip.txt from the upstream GitHub release when missing
#   - builds with Gradle + Android SDK (JDK 17+ and the SDK are auto-installed)
set -euo pipefail
cd "$(dirname "$0")"

ROOT="$(cd .. && pwd)"
export JAVA_HOME="${JAVA_HOME:-}"
if [ -z "$JAVA_HOME" ] || [ ! -x "$JAVA_HOME/bin/java" ]; then
    for cand in /kode/dossier/jdk19 /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/temurin-17-jdk-amd64; do
        if [ -x "$cand/bin/java" ]; then export JAVA_HOME="$cand"; break; fi
    done
fi
if [ -z "$JAVA_HOME" ] || [ ! -x "$JAVA_HOME/bin/java" ]; then
    echo "ERROR: JDK 17+ required (set JAVA_HOME)"; exit 1
fi
export ANDROID_HOME="${ANDROID_HOME:-$HOME/android-sdk}"
export PATH="$JAVA_HOME/bin:$PATH"
GRADLE_HOME="${GRADLE_HOME:-/tmp/gradle-8.9}"

# 1) cross-compile the Go core
echo "== cross-compiling cforward for android/arm64"
(cd "$ROOT" && CGO_ENABLED=0 GOOS=android GOARCH=arm64 go build \
    -ldflags "-s -w -X main.version=$(date +%Y%m%d)-android" \
    -o android/app/src/main/assets/bin/cforward .)

# 2) fetch cfst + ip.txt from the upstream release when missing
if [ ! -f app/src/main/assets/cfst/cfst ] || [ ! -f app/src/main/assets/cfst/ip.txt ]; then
    echo "== fetching cfst_linux_arm64 from GitHub releases"
    cd /tmp
    curl -sL -o cfst_arm64.tar.gz \
        https://github.com/XIU2/CloudflareSpeedTest/releases/download/v2.3.5/cfst_linux_arm64.tar.gz
    tar xzf cfst_arm64.tar.gz -C /tmp
    cp /tmp/cfst_linux_arm64/cfst "$ROOT/android/app/src/main/assets/cfst/"
    cp /tmp/cfst_linux_arm64/ip.txt "$ROOT/android/app/src/main/assets/cfst/"
fi

# 3) ensure the Android SDK is installed
if [ ! -d "$ANDROID_HOME/platforms/android-34" ]; then
    echo "== installing Android SDK"
    mkdir -p "$ANDROID_HOME/cmdline-tools"
    if [ ! -d "$ANDROID_HOME/cmdline-tools/latest" ]; then
        cd /tmp
        [ -f cmdline-tools.zip ] || curl -sL -o cmdline-tools.zip \
            https://dl.google.com/android/repository/commandlinetools-linux-11076708_latest.zip
        unzip -q -o cmdline-tools.zip -d "$ANDROID_HOME/cmdline-tools"
        mv "$ANDROID_HOME/cmdline-tools/cmdline-tools" "$ANDROID_HOME/cmdline-tools/latest"
    fi
    yes | "$ANDROID_HOME/cmdline-tools/latest/bin/sdkmanager" --sdk_root="$ANDROID_HOME" \
        "platforms;android-34" "build-tools;34.0.0" "platform-tools" >/dev/null
fi

# 4) gradle
if [ ! -d "$GRADLE_HOME" ]; then
    cd /tmp
    [ -f gradle-8.9.zip ] || curl -sL -o gradle-8.9.zip \
        "https://services.gradle.org/distributions/gradle-8.9-bin.zip" ||
        curl -sL -o gradle-8.9.zip \
        "https://mirrors.cloud.tencent.com/gradle/gradle-8.9-bin.zip"
    unzip -q -o gradle-8.9.zip
fi

# 5) build
echo "== gradle assembleDebug"
"$GRADLE_HOME/bin/gradle" -p . assembleDebug --no-daemon
echo "== APK: app/build/outputs/apk/debug/app-debug.apk"
