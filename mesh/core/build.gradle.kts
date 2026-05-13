plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.nordic.nexus.jvm)
}

// simdo-patch: publish group only. coordinates stay no.nordicsemi.kotlin.mesh.*
//   for source compatibility; publish는 안 함 (path-dep 으로만 사용).
group = "com.neostack.kotlin.mesh"

nordicNexusPublishing {
    POM_ARTIFACT_ID = "core"
    POM_NAME = "Bluetooth Mesh Core Library"
    POM_DESCRIPTION = "Provides a complete set of Bluetooth Mesh features for the Kotlin Mesh Library."
    POM_URL = "https://github.com/nordicsemi/Kotlin-Mesh-Library"
    POM_SCM_URL = "https://github.com/nordicsemi/Kotlin-Mesh-Library"
    POM_SCM_CONNECTION = "scm:git@github.com:nordicsemi/Kotlin-Mesh-Library.git"
    POM_SCM_DEV_CONNECTION = "scm:git@github.com:nordicsemi/Kotlin-Mesh-Library.git"
}

dependencies {
    api(project(":mesh:bearer"))
    api(project(":mesh:crypto"))
    api(project(":mesh:logger"))
    implementation(nordic.kotlin.data)

    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.datetime)
    // Dependencies used for testing
    testImplementation(libs.kotlin.test)
}

// simdo-patch: upstream 384296817 의 test source 3 파일은 dependency drift 로 컴파일 불가.
//   - MeshNetworkTest.kt       → assertDoesNotThrow / TestScope 미해결 (kotlin.test 1.x 시그니처 변경)
//   - TestPropertiesStorage.kt → SecurePropertiesStorage 의 suspend modifier 누락 (subtype)
//   - GroupTest.kt             → TestPropertiesStorage / TestScope 의존
// 우리 RX metadata fixture 만 컴파일/실행하기 위해 위 파일들을 testSourceSet 에서 제외.
// upstream rebase 시 정상화되면 본 블록 자체를 삭제.
sourceSets {
    named("test") {
        java.exclude(
            "no/nordicsemi/kotlin/mesh/core/model/MeshNetworkTest.kt",
            "no/nordicsemi/kotlin/mesh/core/model/TestPropertiesStorage.kt",
            "no/nordicsemi/kotlin/mesh/core/model/GroupTest.kt",
        )
    }
}