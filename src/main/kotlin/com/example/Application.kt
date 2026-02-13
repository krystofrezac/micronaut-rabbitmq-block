package com.example

import com.rabbitmq.client.Channel
import io.micronaut.rabbitmq.annotation.Queue
import io.micronaut.rabbitmq.annotation.RabbitClient
import io.micronaut.rabbitmq.annotation.RabbitListener
import io.micronaut.rabbitmq.connect.ChannelInitializer
import io.micronaut.runtime.Micronaut.run
import jakarta.inject.Singleton
import org.reactivestreams.Publisher

fun main(args: Array<String>) {
    run(*args)
}


@Singleton
class ChannelPoolListener : ChannelInitializer() { // (2)

    override fun initialize(channel: Channel, name: String) {
//        channel.exchangeDelete("my-exchange")
        channel.exchangeDeclare("my-exchange", "fanout")

//        channel.queueDelete("my-queue")
        channel.queueDeclare("my-queue", false, false, false, emptyMap())

        channel.queueBind("my-queue", "my-exchange", "")
    }
}

@RabbitClient("my-exchange")
interface Producer {
    fun sendReactive(data: String): Publisher<Void>
    suspend fun sendSuspended(data: String)
}


@RabbitListener
class Consumer {

    val received: MutableList<String> = mutableListOf()

    @Queue("my-queue")
    fun receive(data: String) {
        println("Receive")
        received.add(data)
    }
}
