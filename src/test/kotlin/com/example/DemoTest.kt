package com.example

import io.micronaut.context.propagation.slf4j.MdcPropagationContext
import io.micronaut.core.async.propagation.KotlinCoroutinePropagation
import io.micronaut.core.propagation.PropagatedContext
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Test
import jakarta.inject.Inject
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.BeforeEach
import reactor.core.publisher.Mono
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory

const val BLOCK_TIME = 2_000L

@MicronautTest(rebuildContext = true)
class DemoTest {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Inject
    lateinit var producer: Producer

    @Inject
    lateinit var consumer: Consumer

    @BeforeEach
    fun cleanUp() {
        consumer.received.clear()
    }


    // Doesn't work - check logs
    // Notice that `Returned channel` line is logged after the "business" logic finishes
    @Test
    fun reactive() {
        Mono.from(producer.sendReactive("abc"))
            .doOnSuccess {
                logger.info("Starting business logic")
                // Some blocking operation, e.g. DB
                Thread.sleep(BLOCK_TIME)
                logger.info("Ended business logic")

                // It should be already send to RabbitMQ and consumed
                assert(consumer.received.size == 1)
            }
            .subscribe()
    }

    // Doesn't work
    // Notice that `Returned channel` line is logged after the "business" logic finishes
    @Test
    fun `coroutines with context`(): Unit = runBlocking {
        // Needs to have at least one element
        val testContext = PropagatedContext.empty() + MdcPropagationContext(mapOf("a" to "b"))
        withContext(KotlinCoroutinePropagation.addPropagatedContext(currentCoroutineContext(), testContext)) {
            producer.sendSuspended("abc")

            logger.info("Starting business logic")
            // Some blocking operation, e.g. DB
            Thread.sleep(BLOCK_TIME)
            logger.info("Ended business logic")

            // It should be already send to RabbitMQ and consumed
            assert(consumer.received.size == 1)
        }
    }

    // Works
    @Test
    fun `coroutines without context`(): Unit = runBlocking {
        producer.sendSuspended("abc")

        logger.info("Starting business logic")
        // Some blocking operation, e.g. DB
        Thread.sleep(BLOCK_TIME)
        logger.info("Ended business logic")

        assert(consumer.received.size == 1)
    }


    // Works
    @Test
    fun `reactor to coroutines with context`(): Unit = runBlocking {
        // Needs to have at least one element
        val testContext = PropagatedContext.empty() + MdcPropagationContext(mapOf("a" to "b"))
        withContext(KotlinCoroutinePropagation.addPropagatedContext(currentCoroutineContext(), testContext)) {
            producer.sendReactive("abc").awaitFirstOrNull()

            logger.info("Starting business logic")
            // Some blocking operation, e.g. DB
            Thread.sleep(BLOCK_TIME)
            logger.info("Ended business logic")

            // It should be already send to RabbitMQ and consumed
            assert(consumer.received.size == 1)
        }
    }
}
