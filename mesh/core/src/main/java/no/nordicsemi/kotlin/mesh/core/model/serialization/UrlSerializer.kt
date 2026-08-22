package no.nordicsemi.kotlin.mesh.core.model.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.net.URI

/**
 * Custom JSON serializer/deserializer for URI.
 */
// simdo-patch(mesh-dfu backport): upstream 은 `URLSerializer : KSerializer<URL>` 였다.
//   `java.net.URL` 의 equals/hashCode 는 **블로킹 DNS 조회를 수행**한다 (호스트를 IP 로 풀어
//   비교하는 java.net.URL 의 유명한 함정). 이 타입이 들어가는 FirmwareInformation 은
//   `data class` 라 컴파일러가 equals/hashCode 를 생성하므로, Set 에 담거나 두 값을 비교하는
//   순간 Android 메인스레드에서 NetworkOnMainThreadException 이 난다.
//   `java.net.URI` 는 순수 문자열 비교라 안전하고, DFU 의 Update URI 는 어차피 해석하지 않는
//   불투명 식별자다 (NCS dfu_cli.c 도 memcpy 로만 다룬다).
//   파일명은 upstream 경로를 유지해 rebase diff 를 줄인다.
internal object UriSerializer : KSerializer<URI> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("URI", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: URI) {
        encoder.encodeString(value.toString())
    }

    override fun deserialize(decoder: Decoder): URI {
        return URI(decoder.decodeString())
    }
}
