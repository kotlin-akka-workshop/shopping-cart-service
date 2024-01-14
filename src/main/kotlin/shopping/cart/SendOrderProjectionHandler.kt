package shopping.cart

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.javadsl.ClusterSharding
import akka.projection.eventsourced.EventEnvelope
import akka.projection.javadsl.Handler
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shopping.order.proto.Item
import shopping.order.proto.OrderRequest
import shopping.order.proto.OrderResponse
import shopping.order.proto.ShoppingOrderService
import java.time.Duration
import java.util.concurrent.*
import java.util.stream.*


internal class SendOrderProjectionHandler(system: ActorSystem<*>, private val orderService: ShoppingOrderService) :
    Handler<EventEnvelope<ShoppingCart.Event>>() {
    private val log: Logger = LoggerFactory.getLogger(javaClass)
    private val sharding: ClusterSharding = ClusterSharding.get(system)
    private val timeout: Duration = system.settings().config().getDuration("shopping-cart-service.ask-timeout")

    override fun process(envelope: EventEnvelope<ShoppingCart.Event>): CompletionStage<Done> {
        if (envelope.event() is ShoppingCart.CheckedOut) {
            val checkedOut = envelope.event() as ShoppingCart.CheckedOut
            return sendOrder(checkedOut)
        } else {
            return CompletableFuture.completedFuture(Done.done())
        }
    }

    private fun sendOrder(checkout: ShoppingCart.CheckedOut): CompletionStage<Done> {
        val entityRef =
            sharding.entityRefFor(ShoppingCart.getEntityKey(), checkout.getCardId())
        val reply =
            entityRef.ask({ replyTo: ActorRef<ShoppingCart.Summary> ->
                ShoppingCart.Get(
                    replyTo
                )
            }, timeout)
        return reply.thenCompose { cart: ShoppingCart.Summary ->
            val protoItems =
                cart.getItems().entries.stream()
                    .map { entry: Map.Entry<String?, Int?> ->
                        Item.newBuilder()
                            .setItemId(entry.key)
                            .setQuantity(entry.value!!)
                            .build()
                    }
                    .collect(Collectors.toList())
            log.info("Sending order of {} items for cart {}.", cart.getItems().size, checkout.getCardId())
            val orderRequest =
                OrderRequest.newBuilder().setCartId(checkout.getCardId()).addAllItems(protoItems).build()
            orderService.order(orderRequest)
                .thenApply { response: OrderResponse? -> Done.done() }
        }
    }
}
