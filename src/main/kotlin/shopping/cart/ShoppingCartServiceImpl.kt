package shopping.cart

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.DispatcherSelector
import akka.cluster.sharding.typed.javadsl.ClusterSharding
import akka.grpc.GrpcServiceException
import akka.pattern.StatusReply
import io.grpc.Status
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shopping.cart.ShoppingCart.AddItem
import shopping.cart.ShoppingCart.AdjustItemQuantity
import shopping.cart.ShoppingCart.Checkout
import shopping.cart.ShoppingCart.RemoveItem
import shopping.cart.proto.AddItemRequest
import shopping.cart.proto.Cart
import shopping.cart.proto.CheckoutRequest
import shopping.cart.proto.GetCartRequest
import shopping.cart.proto.GetItemPopularityRequest
import shopping.cart.proto.GetItemPopularityResponse
import shopping.cart.proto.Item
import shopping.cart.proto.ShoppingCartService
import shopping.cart.proto.UpdateItemRequest
import shopping.cart.repository.ItemPopularityRepository
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.stream.*


class ShoppingCartServiceImpl(system: ActorSystem<*>, repository: ItemPopularityRepository) :
    ShoppingCartService {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val timeout: Duration
    private val sharding: ClusterSharding


    private val repository: ItemPopularityRepository
    private val blockingJdbcExecutor: Executor

    init {
        val dispatcherSelector =
            DispatcherSelector.fromConfig("akka.projection.jdbc.blocking-jdbc-dispatcher")
        this.blockingJdbcExecutor = system.dispatchers().lookup(dispatcherSelector)

        this.repository = repository
        timeout = system.settings().config().getDuration("shopping-cart-service.ask-timeout")
        sharding = ClusterSharding.get(system)
    }


    override fun addItem(input: AddItemRequest): CompletionStage<Cart> {
        logger.info("addItem {} to cart {}", input.itemId, input.cartId)
        val entityRef =
            sharding.entityRefFor(ShoppingCart.getEntityKey(), input.cartId)
        val reply =
            entityRef.askWithStatus(
                { replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                    AddItem(
                        input.itemId,
                        input.quantity,
                        replyTo
                    )
                },
                timeout
            )
        val cart = reply.thenApply { cart: ShoppingCart.Summary ->
            toProtoCart(
                cart
            )
        }
        return convertError(cart)
    }

    override fun updateItem(input: UpdateItemRequest): CompletionStage<Cart> {
        logger.info("getCart {}", input.cartId)
        val entityRef =
            sharding.entityRefFor(ShoppingCart.getEntityKey(), input.cartId)
        val reply: CompletionStage<ShoppingCart.Summary> = if (input.quantity == 0) {
            entityRef.askWithStatus(
                { replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                    RemoveItem(
                        input.itemId,
                        replyTo
                    )
                }, timeout
            )
        } else {
            entityRef.askWithStatus(
                { replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                    AdjustItemQuantity(
                        input.itemId,
                        input.quantity,
                        replyTo
                    )
                },
                timeout
            )
        }
        val cart = reply.thenApply { cart: ShoppingCart.Summary ->
            toProtoCart(
                cart
            )
        }
        return convertError(cart)
    }


    override fun checkout(input: CheckoutRequest): CompletionStage<Cart> {
        logger.info("checkout {}", input.cartId)
        val entityRef =
            sharding.entityRefFor(ShoppingCart.getEntityKey(), input.cartId)
        val reply =
            entityRef.askWithStatus({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                Checkout(
                    replyTo
                )
            }, timeout)
        val cart = reply.thenApply { cart: ShoppingCart.Summary ->
            toProtoCart(
                cart
            )
        }
        return convertError(cart)
    }

    override fun getCart(input: GetCartRequest): CompletionStage<Cart> {
        logger.info("getCart {}", input.cartId)
        val entityRef =
            sharding.entityRefFor(ShoppingCart.getEntityKey(), input.cartId)
        val reply =
            entityRef.ask({ replyTo: ActorRef<ShoppingCart.Summary> ->
                ShoppingCart.Get(
                    replyTo
                )
            }, timeout)
        val protoCart =
            reply.thenApply { cart: ShoppingCart.Summary ->
                if (cart.getItems().isEmpty()) throw GrpcServiceException(
                    Status.NOT_FOUND.withDescription("Cart " + input.cartId + " not found")
                )
                else return@thenApply toProtoCart(cart)
            }
        return convertError(protoCart)
    }


    override fun getItemPopularity(input: GetItemPopularityRequest): CompletionStage<GetItemPopularityResponse> {
        val itemPopularity: CompletionStage<Optional<ItemPopularity>> =
            CompletableFuture.supplyAsync(
                { repository.findById(input.itemId) }, blockingJdbcExecutor
            )

        return itemPopularity.thenApply { popularity: Optional<ItemPopularity> ->
            val count = popularity.map(ItemPopularity::count).orElse(0L)
            GetItemPopularityResponse.newBuilder().setPopularityCount(count).build()
        }
    }


    companion object {
        private fun toProtoCart(cart: ShoppingCart.Summary): Cart {
            val protoItems =
                cart.getItems().entries.stream()
                    .map { entry: Map.Entry<String?, Int?> ->
                        Item.newBuilder()
                            .setItemId(entry.key)
                            .setQuantity(entry.value!!)
                            .build()
                    }
                    .collect(Collectors.toList())

            return Cart.newBuilder().setCheckedOut(cart.isCheckedOut()).addAllItems(protoItems).build()
        }


        private fun <T> convertError(response: CompletionStage<T>): CompletionStage<T> {
            return response.exceptionally { ex: Throwable ->
                if (ex is TimeoutException) {
                    throw GrpcServiceException(
                        Status.UNAVAILABLE.withDescription("Operation timed out")
                    )
                } else {
                    throw GrpcServiceException(
                        Status.INVALID_ARGUMENT.withDescription(ex.message)
                    )
                }
            }
        }
    }
}
