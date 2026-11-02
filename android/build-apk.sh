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
mkdir -p "$ROOT/android/app/src/main/assets/bin" "$ROOT/android/app/src/main/assets/cfst"
(cd "$ROOT" && CGO_ENABLED=0 GOOS=android GOARCH=arm64 go build \
    -ldflags "-s -w -X main.version=$(date +%Y%m%d)-android" \
    -o android/app/src/main/assets/bin/cforward .)

# 2) build cfst from source as a PIE binary. The official releases are static
# non-PIE ELFs that Android's linker64 cannot load on hardened ROMs (SELinux
# forbids execve of app-data files); a Go android/arm64 build is PIE and works
# via the same linker64 loading as the cforward core.
CFST_SRC=/tmp/cfst-src
if ! file app/src/main/assets/cfst/cfst 2>/dev/null | grep -q "pie executable"; then
    echo "== building cfst (android/arm64 PIE) from source"
    if [ ! -f "$CFST_SRC/go.mod" ]; then
        rm -rf "$CFST_SRC" /tmp/cfst-src.tgz
        echo "== fetching cfst source (v2.3.5)"
        (curl -sL -o /tmp/cfst-src.tgz \
            "https://ghproxy.net/https://github.com/XIU2/CloudflareSpeedTest/archive/refs/tags/v2.3.5.tar.gz" ||
         curl -sL -o /tmp/cfst-src.tgz \
            "https://github.com/XIU2/CloudflareSpeedTest/archive/refs/tags/v2.3.5.tar.gz") || exit 1
        mkdir -p "$CFST_SRC"
        tar xzf /tmp/cfst-src.tgz -C "$CFST_SRC" --strip-components=1
    fi
    (cd "$CFST_SRC" && CGO_ENABLED=0 GOOS=android GOARCH=arm64 go build \
        -ldflags "-s -w" -o "$ROOT/android/app/src/main/assets/cfst/cfst" .) || exit 1
    cp "$CFST_SRC/ip.txt" "$ROOT/android/app/src/main/assets/cfst/ip.txt"
fi
# GPL-3.0 license text for the bundled third-party binary (kept in git)
[ -f "$ROOT/android/app/src/main/assets/cfst/LICENSE.txt" ] || \
    curl -sL -o "$ROOT/android/app/src/main/assets/cfst/LICENSE.txt" \
    "https://raw.githubusercontent.com/XIU2/CloudflareSpeedTest/master/LICENSE"

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

# 5) build (re-enter the script dir: earlier steps may have changed the cwd)
cd "$(dirname "$0")"
echo "== gradle assembleDebug"
"$GRADLE_HOME/bin/gradle" -p . assembleDebug --no-daemon
echo "== APK: app/build/outputs/apk/debug/app-debug.apk"
