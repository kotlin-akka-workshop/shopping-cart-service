package shopping.cart

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.javadsl.ActorContext
import akka.actor.typed.javadsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.javadsl.ClusterSharding
import akka.cluster.sharding.typed.javadsl.Entity
import akka.cluster.sharding.typed.javadsl.EntityContext
import akka.cluster.sharding.typed.javadsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.javadsl.CommandHandlerWithReply
import akka.persistence.typed.javadsl.CommandHandlerWithReplyBuilderByState
import akka.persistence.typed.javadsl.EventHandler
import akka.persistence.typed.javadsl.EventSourcedBehaviorWithEnforcedReplies
import akka.persistence.typed.javadsl.ReplyEffect
import akka.persistence.typed.javadsl.RetentionCriteria
import com.fasterxml.jackson.annotation.JsonCreator
import java.time.Duration
import java.time.Instant
import java.util.Optional
import kotlin.math.abs


/**
 * This is an event sourced actor (`EventSourcedBehavior`). An entity managed by Cluster Sharding.
 *
 *
 * It has a state, [[ShoppingCart.State]], which holds the current shopping cart items and
 * whether it's checked out.
 *
 *
 * You interact with event sourced actors by sending commands to them, see classes implementing
 * [[ShoppingCart.Command]].
 *
 *
 * The command handler validates and translates commands to events, see classes implementing
 * [[ShoppingCart.Event]]. It's the events that are persisted by the `EventSourcedBehavior`. The
 * event handler updates the current state based on the event. This is done when the event is first
 * created, and when the entity is loaded from the database - each event will be replayed to
 * recreate the state of the entity.
 */
class ShoppingCart
private constructor(private val cartId: String, private val projectionTag: String) :
    EventSourcedBehaviorWithEnforcedReplies<ShoppingCart.Command, ShoppingCart.Event, ShoppingCart.State>(
        PersistenceId.of(ENTITY_KEY.name(), cartId),
        SupervisorStrategy.restartWithBackoff(Duration.ofMillis(200), Duration.ofSeconds(5), 0.1)
    ) {
    /** The current state held by the `EventSourcedBehavior`.  */
    class State @JvmOverloads constructor(
        val items: MutableMap<String, Int> = HashMap(),
        private var checkoutDate: Optional<Instant> = Optional.empty()
    ) : CborSerializable {
        val isCheckedOut: Boolean
            get() = checkoutDate.isPresent

        fun checkout(now: Instant?): State {
            checkoutDate = Optional.of(now!!)
            return this
        }

        fun toSummary(): Summary {
            return Summary(items, isCheckedOut)
        }

        fun hasItem(itemId: String): Boolean {
            return items.containsKey(itemId)
        }

        fun updateItem(itemId: String, quantity: Int): State {
            if (quantity == 0) {
                items.remove(itemId)
            } else {
                items[itemId] = quantity
            }
            return this
        }

        val isEmpty: Boolean
            get() = items.isEmpty()


        fun removeItem(itemId: String): State {
            items.remove(itemId)
            return this
        }

        fun itemCount(itemId: String): Int {
            return items[itemId]!!
        }
    }


    /** This interface defines all the commands (messages) that the ShoppingCart actor supports.  */
    interface Command : CborSerializable

    /**
     * A command to add an item to the cart.
     *
     *
     * It replies with `StatusReply&lt;Summary&gt;`, which is sent back to the caller when all the
     * events emitted by this command are successfully persisted.
     */
    data class AddItem(val itemId: String, val quantity: Int, val replyTo: ActorRef<StatusReply<Summary>>) : Command

    /** A command to remove an item from the cart.  */
    data class RemoveItem(val itemId: String, val replyTo: ActorRef<StatusReply<Summary>>) : Command

    /** A command to adjust the quantity of an item in the cart.  */
    data class AdjustItemQuantity(val itemId: String, val quantity: Int, val replyTo: ActorRef<StatusReply<Summary>>) :
        Command

    /** A command to checkout the shopping cart.  */
    data class Checkout @JsonCreator constructor(val replyTo: ActorRef<StatusReply<Summary>>) : Command

    /** A command to get the current state of the shopping cart.  */
    data class Get @JsonCreator constructor(val replyTo: ActorRef<Summary>) : Command

    /** Summary of the shopping cart state, used in reply messages.  */
    class Summary(val items: Map<String, Int>, val checkedOut: Boolean) : CborSerializable

    interface Event : CborSerializable {
        val cartId: String
    }

    internal data class ItemAdded(override val cartId: String, val itemId: String, val quantity: Int) : Event

    internal data class ItemRemoved(override val cartId: String, val itemId: String, val oldQuantity: Int) : Event

    internal data class ItemQuantityAdjusted(
        override val cartId: String,
        val itemId: String,
        val oldQuantity: Int,
        val newQuantity: Int
    ) : Event

    internal data class CheckedOut(override val cartId: String, val eventTime: Instant) : Event

    override fun tagsFor(event: Event): Set<String> {
        return setOf(projectionTag)
    }


    override fun retentionCriteria(): RetentionCriteria {
        return RetentionCriteria.snapshotEvery(100, 3)
    }

    override fun emptyState(): State {
        return State()
    }

    override fun commandHandler(): CommandHandlerWithReply<Command, Event, State> {
        return openShoppingCart().orElse(checkedOutShoppingCart()).orElse(commandHandler).build()
    }

    private fun openShoppingCart(): CommandHandlerWithReplyBuilderByState<Command, Event, State, State> {
        return newCommandHandlerWithReplyBuilder()
            .forState({ state: State -> !state.isCheckedOut })
            .onCommand(
                AddItem::class.java,
                { state: State, cmd: AddItem ->
                    this.onAddItem(
                        state,
                        cmd
                    )
                })
            .onCommand(
                RemoveItem::class.java,
                { state: State, cmd: RemoveItem ->
                    this.onRemoveItem(
                        state,
                        cmd
                    )
                })
            .onCommand(
                AdjustItemQuantity::class.java,
                { state: State, cmd: AdjustItemQuantity ->
                    this.onAdjustItemQuantity(
                        state,
                        cmd
                    )
                })
            .onCommand(
                Checkout::class.java,
                { state: State, cmd: Checkout ->
                    this.onCheckout(
                        state,
                        cmd
                    )
                })
    }

    private fun onAddItem(state: State, cmd: AddItem): ReplyEffect<Event?, State?> {
        if (state.hasItem(cmd.itemId)) {
            return Effect()
                .reply(
                    cmd.replyTo,
                    StatusReply.error(
                        "Item '" + cmd.itemId + "' was already added to this shopping cart"
                    )
                )
        } else if (cmd.quantity <= 0) {
            return Effect().reply(cmd.replyTo, StatusReply.error("Quantity must be greater than zero"))
        } else {
            return Effect()
                .persist(ItemAdded(cartId, cmd.itemId, cmd.quantity))
                .thenReply(cmd.replyTo,
                    { updatedCart: State ->
                        StatusReply.success(
                            updatedCart.toSummary()
                        )
                    })
        }
    }

    private fun onCheckout(state: State, cmd: Checkout): ReplyEffect<Event?, State?> {
        if (state.isEmpty) {
            return Effect()
                .reply(cmd.replyTo, StatusReply.error("Cannot checkout an empty shopping cart"))
        } else {
            return Effect()
                .persist(CheckedOut(cartId, Instant.now()))
                .thenReply(cmd.replyTo,
                    { updatedCart: State ->
                        StatusReply.success(
                            updatedCart.toSummary()
                        )
                    })
        }
    }

    private fun onRemoveItem(state: State, cmd: RemoveItem): ReplyEffect<Event?, State?> {
        if (state.hasItem(cmd.itemId)) {
            return Effect()
                .persist(ItemRemoved(cartId, cmd.itemId, state.itemCount(cmd.itemId)))
                .thenReply(cmd.replyTo,
                    { updatedCart: State ->
                        StatusReply.success(
                            updatedCart.toSummary()
                        )
                    })
        } else {
            return Effect()
                .reply(
                    cmd.replyTo,
                    StatusReply.success(state.toSummary())
                ) // removing an item is idempotent
        }
    }

    private fun onAdjustItemQuantity(state: State, cmd: AdjustItemQuantity): ReplyEffect<Event?, State?> {
        if (cmd.quantity <= 0) {
            return Effect().reply(cmd.replyTo, StatusReply.error("Quantity must be greater than zero"))
        } else if (state.hasItem(cmd.itemId)) {
            return Effect()
                .persist(
                    ItemQuantityAdjusted(
                        cartId, cmd.itemId, state.itemCount(cmd.itemId), cmd.quantity
                    )
                )
                .thenReply(cmd.replyTo,
                    { updatedCart: State ->
                        StatusReply.success(
                            updatedCart.toSummary()
                        )
                    })
        } else {
            return Effect()
                .reply(
                    cmd.replyTo,
                    StatusReply.error(
                        "Cannot adjust quantity for item '"
                                + cmd.itemId
                                + "'. Item not present on cart"
                    )
                )
        }
    }

    private fun checkedOutShoppingCart(): CommandHandlerWithReplyBuilderByState<Command, Event, State, State> {
        return newCommandHandlerWithReplyBuilder()
            .forState({ obj: State -> obj.isCheckedOut })
            .onCommand(
                AddItem::class.java,
                { cmd: AddItem ->
                    Effect()
                        .reply(
                            cmd.replyTo,
                            StatusReply.error(
                                "Can't add an item to an already checked out shopping cart"
                            )
                        )
                })
            .onCommand(
                RemoveItem::class.java,
                { cmd: RemoveItem ->
                    Effect()
                        .reply(
                            cmd.replyTo,
                            StatusReply.error(
                                "Can't remove an item from an already checked out shopping cart"
                            )
                        )
                })
            .onCommand(
                AdjustItemQuantity::class.java,
                { cmd: AdjustItemQuantity ->
                    Effect()
                        .reply(
                            cmd.replyTo,
                            StatusReply.error(
                                "Can't adjust item on an already checked out shopping cart"
                            )
                        )
                })
            .onCommand(
                Checkout::class.java,
                { cmd: Checkout ->
                    Effect()
                        .reply(
                            cmd.replyTo,
                            StatusReply.error("Can't checkout already checked out shopping cart")
                        )
                })
    }

    private val commandHandler: CommandHandlerWithReplyBuilderByState<Command, Event, State, State>
        get() = newCommandHandlerWithReplyBuilder()
            .forAnyState()
            .onCommand(
                Get::class.java,
                { state: State, cmd: Get ->
                    Effect().reply(
                        cmd.replyTo,
                        state.toSummary()
                    )
                })

    override fun eventHandler(): EventHandler<State, Event> {
        return newEventHandlerBuilder()
            .forAnyState()
            .onEvent(
                ItemAdded::class.java,
                { state: State, evt: ItemAdded ->
                    state.updateItem(
                        evt.itemId,
                        evt.quantity
                    )
                })
            .onEvent(
                ItemRemoved::class.java,
                { state: State, evt: ItemRemoved ->
                    state.removeItem(
                        evt.itemId
                    )
                })
            .onEvent(
                ItemQuantityAdjusted::class.java,
                { state: State, evt: ItemQuantityAdjusted ->
                    state.updateItem(
                        evt.itemId,
                        evt.newQuantity
                    )
                })
            .onEvent(
                CheckedOut::class.java,
                { state: State, evt: CheckedOut ->
                    state.checkout(
                        evt.eventTime
                    )
                })
            .build()
    }

    companion object {

        val ENTITY_KEY: EntityTypeKey<Command> = EntityTypeKey.create(Command::class.java, "ShoppingCart")

        //TODO what are tags ?
        val TAGS: List<String> = listOf("carts-0", "carts-1", "carts-2", "carts-3", "carts-4")

        fun init(system: ActorSystem<*>?): ActorRef<ShardingEnvelope<Command>> = ClusterSharding.get(system)
            .init(
                Entity.of(
                    ENTITY_KEY,
                    { entityContext: EntityContext<Command?> ->
                        val i: Int =
                            abs(
                                (entityContext.entityId.hashCode() % TAGS.size).toDouble()
                            )
                                .toInt()
                        val selectedTag: String = TAGS.get(i)
                        create(entityContext.entityId, selectedTag)
                    })
            )


        fun create(cartId: String, projectionTag: String): Behavior<Command?> = Behaviors.setup(
            { ctx: ActorContext<Command?>? ->
                start(
                    ShoppingCart(cartId, projectionTag),
                    ctx
                )
            })
    }
}
