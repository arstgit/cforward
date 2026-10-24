plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.cforward.app"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.cforward.app"
        minSdk = 26
        targetSdk = 34
        versionCode = 1
        versionName = "1.0.0"
    }

    buildTypes {
        release {
            isMinifyEnabled = false
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions {
        jvmTarget = "17"
    }
}

// No external runtime dependencies: plain platform APIs only, so the APK builds
// and runs even without androidx (the UI is a simple polling status screen).
dependencies {
}
