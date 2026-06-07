pluginManagement {
    repositories {
        mavenLocal()
        gradlePluginPortal()
        google()
        mavenCentral()
    }
}
dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        mavenLocal()
        google()
        mavenCentral()
        maven(url = "https://jitpack.io")
        maven(url = "https://androidx.dev/storage/compose-compiler/repository/")
    }
    versionCatalogs {
        create("libs") {
             from("no.nordicsemi.android.gradle:version-catalog:2.15")
             // simdo-patch: 메인 빌드(android/)는 AGP 9.2.1 사용. composite build 는
             //   모든 included build 의 AGP 버전이 동일해야 함("Using multiple versions
             //   of AGP across Gradle builds is not allowed"). Nordic catalog 2.15 의
             //   androidGradlePlugin 9.1.0 을 9.2.1 로 override 하여 정렬.
             //   nordicPlugins(2.15) 는 그대로 유지 → convention plugin 영향 없음.
             //   소비되는 :mesh:* 모듈은 kotlin.jvm + nordic.nexus.jvm 만 적용하여
             //   AGP 미사용 — 이 override 는 비소비 :app/:feature 모듈에만 작용.
             version("androidGradlePlugin", "9.2.1")
        }
        create("nordic") {
            from("no.nordicsemi.android:version-catalog:2025.12.01")
        }
    }
}
rootProject.name = "Kotlin-Mesh-Library"
include(":app")
include(":core:ui")
include(":core:common")
include(":core:data")
include(":core:navigation")
include(":feature:nodes")
include(":feature:models")
include(":feature:groups")
include(":feature:settings")
include(":feature:proxy")
include(":feature:export")
include(":feature:network-keys")
include(":feature:config-network-keys")
include(":feature:application-keys")
include(":feature:bind-app-keys")
include(":feature:config-application-keys")
include(":feature:provisioners")
include(":feature:provisioning")
include(":feature:scenes")
include(":feature:ivindex")
include(":feature:developer-settings")

include(":mesh:core")
include(":mesh:crypto")
include(":mesh:provisioning")
include(":mesh:logger")
include(":mesh:bearer")
include(":mesh:bearer-provisioning")
include(":mesh:bearer-pbgatt")
include(":mesh:bearer-gatt")

// if (file("../Android-Common-Libraries").exists()) {
//     includeBuild("../Android-Common-Libraries")
// }
// if (file("../Android-Gradle-Plugins").exists()) {
//     includeBuild("../Android-Gradle-Plugins")
// }
// if (file("../Kotlin-Util-Library").exists()) {
//     includeBuild("../Kotlin-Util-Library")
// }
// if (file("../Kotlin-BLE-Library").exists()) {
//     includeBuild("../Kotlin-BLE-Library")
// }
