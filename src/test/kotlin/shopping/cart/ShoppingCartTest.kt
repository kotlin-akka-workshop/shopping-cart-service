package shopping.cart

import akka.actor.testkit.typed.javadsl.TestKitJunitResource
import akka.actor.typed.ActorRef
import akka.pattern.StatusReply
import akka.persistence.testkit.javadsl.EventSourcedBehaviorTestKit
import com.typesafe.config.ConfigFactory
import org.junit.Assert
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import shopping.cart.ShoppingCart.AddItem
import shopping.cart.ShoppingCart.AdjustItemQuantity
import shopping.cart.ShoppingCart.Checkout
import shopping.cart.ShoppingCart.Companion.create
import shopping.cart.ShoppingCart.RemoveItem


class ShoppingCartTest {
    private val eventSourcedTestKit: EventSourcedBehaviorTestKit<ShoppingCart.Command?, ShoppingCart.Event, ShoppingCart.State> =
        EventSourcedBehaviorTestKit.create(
            testKit().system(), create(CART_ID, "carts-0")
        )

    @Before
    fun beforeEach() {
        eventSourcedTestKit.clear()
    }

    @Test
    fun addAnItemToCart() {
        val result =
            eventSourcedTestKit.runCommand({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>>? ->
                AddItem(
                    "foo", 42,
                    (replyTo)!!
                )
            })
        Assert.assertTrue(result.reply().isSuccess)
        val summary = result.reply().value
        Assert.assertFalse(summary.isCheckedOut())
        Assert.assertEquals(1, summary.getItems().size.toLong())
        Assert.assertEquals(42, summary.getItems()["foo"]!!.toLong())
        Assert.assertEquals(ShoppingCart.ItemAdded(CART_ID, "foo", 42), result.event())
    }

    @Test
    fun rejectAlreadyAddedItem() {
        val result1 =
            eventSourcedTestKit.runCommand({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                AddItem(
                    "foo",
                    42,
                    replyTo
                )
            })
        Assert.assertTrue(result1.reply().isSuccess)
        val result2 =
            eventSourcedTestKit.runCommand({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                AddItem(
                    "foo",
                    42,
                    replyTo
                )
            })
        Assert.assertTrue(result2.reply().isError)
        Assert.assertTrue(result2.hasNoEvents())
    }

    @Test
    fun removeItem() {
        val result1 =
            eventSourcedTestKit.runCommand({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                AddItem(
                    "foo",
                    42,
                    replyTo
                )
            })
        Assert.assertTrue(result1.reply().isSuccess)
        val result2 =
            eventSourcedTestKit.runCommand({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                RemoveItem(
                    "foo",
                    replyTo
                )
            })
        Assert.assertTrue(result2.reply().isSuccess)
        Assert.assertEquals(ShoppingCart.ItemRemoved(CART_ID, "foo", 42), result2.event())
    }

    @Test
    fun adjustQuantity() {
        val result1 =
            eventSourcedTestKit.runCommand({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                AddItem(
                    "foo",
                    42,
                    replyTo
                )
            })
        Assert.assertTrue(result1.reply().isSuccess)
        val result2 =
            eventSourcedTestKit.runCommand(
                { replyTo: ActorRef<StatusReply<ShoppingCart.Summary>>? ->
                    AdjustItemQuantity(
                        "foo", 43,
                        (replyTo)!!
                    )
                })
        Assert.assertTrue(result2.reply().isSuccess)
        Assert.assertEquals(43, result2.reply().value.getItems()["foo"]!!.toLong())
        Assert.assertEquals(ShoppingCart.ItemQuantityAdjusted(CART_ID, "foo", 42, 43), result2.event())
    }


    @Test
    fun checkout() {
        val result1 =
            eventSourcedTestKit.runCommand({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                AddItem(
                    "foo",
                    42,
                    replyTo
                )
            })
        Assert.assertTrue(result1.reply().isSuccess)
        val result2 = eventSourcedTestKit.runCommand({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
            Checkout(
                replyTo
            )
        })
        Assert.assertTrue(result2.reply().isSuccess)
        Assert.assertTrue(result2.event() is ShoppingCart.CheckedOut)
        Assert.assertEquals(CART_ID, result2.event().getCardId())

        val result3 =
            eventSourcedTestKit.runCommand({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                AddItem(
                    "foo",
                    42,
                    replyTo
                )
            })
        Assert.assertTrue(result3.reply().isError)
    }


    @Test
    fun get() {
        val result1 =
            eventSourcedTestKit.runCommand({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                AddItem(
                    "foo",
                    42,
                    replyTo
                )
            })
        Assert.assertTrue(result1.reply().isSuccess)

        val result2 = eventSourcedTestKit.runCommand({ replyTo: ActorRef<ShoppingCart.Summary>? ->
            ShoppingCart.Get(
                (replyTo)!!
            )
        })
        Assert.assertFalse(result2.reply().isCheckedOut())
        Assert.assertEquals(1, result2.reply().getItems().size.toLong())
        Assert.assertEquals(42, result2.reply().getItems()["foo"]!!.toLong())
    }


    @Test
    fun keepItsState() {
        val result1 =
            eventSourcedTestKit.runCommand({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                AddItem(
                    "foo",
                    42,
                    replyTo
                )
            })
        Assert.assertTrue(result1.reply().isSuccess)

        eventSourcedTestKit.restart()

        val result2 = eventSourcedTestKit.runCommand({ replyTo: ActorRef<ShoppingCart.Summary>? ->
            ShoppingCart.Get(
                (replyTo)!!
            )
        })
        Assert.assertFalse(result2.reply().isCheckedOut())
        Assert.assertEquals(1, result2.reply().getItems().size.toLong())
        Assert.assertEquals(42, result2.reply().getItems()["foo"]!!.toLong())
    }

    companion object {
        private val CART_ID = "testCart"

        @ClassRule
        @JvmStatic
        fun testKit(): TestKitJunitResource = TestKitJunitResource(
            ConfigFactory.parseString(
                "akka.actor.serialization-bindings {\n"
                        + "  \"shopping.cart.CborSerializable\" = jackson-cbor\n"
                        + "}"
            )
                .withFallback(EventSourcedBehaviorTestKit.config())
        )
    }
}
