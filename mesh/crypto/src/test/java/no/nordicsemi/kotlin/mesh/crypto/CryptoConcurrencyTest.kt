package no.nordicsemi.kotlin.mesh.crypto

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

/**
 * 2026-09-29 실기기 결함 재현 — 여러 스레드가 동시에 AES-CCM·CMAC 을 돌리면 결과가 깨지면 안 된다.
 *
 * 커미셔닝 앱이 설정 링크 둘(+ 프로비저닝 링크)을 동시에 돌리자 정상 응답이 폰에서 `Failed to decrypt network pdu` /
 * `Failed to decode PDU` 로 버려져 설정 메시지가 5 s 재전송을 기다렸고(30회 중 10건), 프로비저닝 한 건은 조명이
 * `Decryption failed` 로 거절했다. 원인은 `Crypto` 가 AES 엔진 하나(`AESEngine`)를 모든 호출에 공유한 것 — CCM·CMAC 의
 * `init(key)` 가 다른 스레드가 쓰는 도중의 키 상태를 바꾼다.
 */
@OptIn(ExperimentalStdlibApi::class)
class CryptoConcurrencyTest {

    @Test
    fun concurrentCcmRoundTripsAndCmacStayCorrect() {
        val threads = 8
        val iterations = 3_000
        val pool = Executors.newFixedThreadPool(threads)
        val start = CountDownLatch(1)
        val failures = AtomicInteger(0)
        val done = CountDownLatch(threads)
        repeat(threads) { t ->
            pool.execute {
                val rnd = Random(t)
                try {
                    start.await()
                    repeat(iterations) {
                        val key = rnd.nextBytes(16)
                        val nonce = rnd.nextBytes(13)
                        val data = rnd.nextBytes(1 + rnd.nextInt(24))
                        val sealed = Crypto.encrypt(data = data, key = key, nonce = nonce, micSize = 4)
                        val opened = Crypto.decrypt(data = sealed, key = key, nonce = nonce, micSize = 4)
                        if (opened == null || !opened.contentEquals(data)) failures.incrementAndGet()
                        // CMAC 경로(k3 = Network ID) 도 같은 엔진을 썼다 — 같은 입력은 같은 값이어야 한다.
                        if (!Crypto.calculateNetworkId(key).contentEquals(Crypto.calculateNetworkId(key))) {
                            failures.incrementAndGet()
                        }
                    }
                } catch (e: Throwable) {
                    failures.incrementAndGet()
                } finally {
                    done.countDown()
                }
            }
        }
        start.countDown()
        done.await(120, TimeUnit.SECONDS)
        pool.shutdownNow()
        assertEquals("동시 호출에서 깨진 암·복호/CMAC 수", 0, failures.get())
    }

    @Test
    fun singleThreadVectorStillMatchesAfterConcurrentUse() {
        // Mesh Profile 8.1.3 k3 샘플 — 동시 사용 뒤에도 표준 벡터가 그대로여야 한다.
        val n = "f7a2a44f8e8a8029064f173ddc1e2b00".hexToByteArray()
        assertArrayEquals("ff046958233db014".hexToByteArray(), Crypto.calculateNetworkId(n))
    }
}
