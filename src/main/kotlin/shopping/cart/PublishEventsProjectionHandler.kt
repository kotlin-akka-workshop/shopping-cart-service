package shopping.cart

import akka.Done
import akka.kafka.javadsl.SendProducer
import akka.projection.eventsourced.EventEnvelope
import akka.projection.javadsl.Handler
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shopping.cart.proto.CheckedOut
import shopping.cart.proto.ItemAdded
import shopping.cart.proto.ItemQuantityAdjusted
import shopping.cart.proto.ItemRemoved
import java.util.concurrent.*


internal class PublishEventsProjectionHandler
    (private val topic: String, private val sendProducer: SendProducer<String, ByteArray>) :
    Handler<EventEnvelope<ShoppingCart.Event>>() {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    override fun process(envelope: EventEnvelope<ShoppingCart.Event>): CompletionStage<Done> {
        val event = envelope.event()

        // using the cartId as the key and `DefaultPartitioner` will select partition based on the key
        // so that events for same cart always ends up in same partition
        val key = event.cartId
        val producerRecord =
            ProducerRecord(topic, key, serialize(event))
        return sendProducer
            .send(producerRecord)
            .thenApply { recordMetadata: RecordMetadata ->
                logger.info(
                    "Published event [{}] to topic/partition {}/{}",
                    event,
                    topic,
                    recordMetadata.partition()
                )
                Done.done()
            }
    }

    companion object {
        private fun serialize(event: ShoppingCart.Event): ByteArray {
            val protoMessage: ByteString
            val fullName: String
            if (event is ShoppingCart.ItemAdded) {
                val itemAdded = event
                protoMessage =
                    ItemAdded.newBuilder()
                        .setCartId(itemAdded.cartId)
                        .setItemId(itemAdded.itemId)
                        .setQuantity(itemAdded.quantity)
                        .build()
                        .toByteString()
                fullName = ItemAdded.getDescriptor().fullName
            } else if (event is ShoppingCart.ItemQuantityAdjusted) {
                val itemQuantityAdjusted =
                    event
                protoMessage =
                    ItemQuantityAdjusted.newBuilder()
                        .setCartId(itemQuantityAdjusted.cartId)
                        .setItemId(itemQuantityAdjusted.itemId)
                        .setQuantity(itemQuantityAdjusted.newQuantity)
                        .build()
                        .toByteString()
                fullName = ItemQuantityAdjusted.getDescriptor().fullName
            } else if (event is ShoppingCart.ItemRemoved) {
                val itemRemoved = event
                protoMessage =
                    ItemRemoved.newBuilder()
                        .setCartId(itemRemoved.cartId)
                        .setItemId(itemRemoved.itemId)
                        .build()
                        .toByteString()
                fullName = ItemRemoved.getDescriptor().fullName
            } else if (event is ShoppingCart.CheckedOut) {
                protoMessage =
                    CheckedOut.newBuilder()
                        .setCartId(event.cartId)
                        .build()
                        .toByteString()
                fullName = CheckedOut.getDescriptor().fullName
            } else {
                throw IllegalArgumentException("Unknown event type: " + event.javaClass)
            }
            // pack in Any so that type information is included for deserialization
            return Any.newBuilder()
                .setValue(protoMessage)
                .setTypeUrl("shopping-cart-service/$fullName")
                .build()
                .toByteArray()
        }
    }
}

