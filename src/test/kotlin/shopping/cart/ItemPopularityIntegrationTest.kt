package shopping.cart

import akka.actor.testkit.typed.javadsl.TestKitJunitResource
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.javadsl.ClusterSharding
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.pattern.StatusReply
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.springframework.orm.jpa.JpaTransactionManager
import shopping.cart.CreateTableTestUtils.createTables
import shopping.cart.ItemPopularityProjection.init
import shopping.cart.ShoppingCart.AddItem
import shopping.cart.ShoppingCart.Companion.getEntityKey
import shopping.cart.ShoppingCart.Companion.init
import shopping.cart.repository.ItemPopularityRepository
import shopping.cart.repository.SpringIntegration.applicationContext
import java.time.Duration
import java.util.concurrent.*


class ItemPopularityIntegrationTest {
    @Test
    @Throws(Exception::class)
    fun consumeCartEventsAndUpdatePopularityCount() {
        val sharding = ClusterSharding.get(system)

        val cartId1 = "cart1"
        val cartId2 = "cart2"
        val item1 = "item1"
        val item2 = "item2"

        val cart1 = sharding.entityRefFor(getEntityKey(), cartId1)
        val cart2 = sharding.entityRefFor(getEntityKey(), cartId2)

        val timeout = Duration.ofSeconds(3)

        val reply1 =
            cart1.askWithStatus({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                AddItem(
                    item1,
                    3,
                    replyTo
                )
            }, timeout)
        val summary1 = reply1.toCompletableFuture()[3, TimeUnit.SECONDS]
        Assert.assertEquals(3, summary1.getItems()[item1]!!.toLong())

        val probe = testKit().createTestProbe<Any>()
        probe.awaitAssert<Any?> {
            val item1Popularity =
                itemPopularityRepository!!.findById(item1)
            Assert.assertTrue(item1Popularity!!.isPresent)
            Assert.assertEquals(3L, item1Popularity.get().count)
            null
        }

        val reply2 =
            cart1.askWithStatus({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                AddItem(
                    item2,
                    5,
                    replyTo
                )
            }, timeout)
        val summary2 = reply2.toCompletableFuture()[3, TimeUnit.SECONDS]
        Assert.assertEquals(2, summary2.getItems().size.toLong())
        Assert.assertEquals(5, summary2.getItems()[item2]!!.toLong())
        // another cart
        val reply3 =
            cart2.askWithStatus({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                AddItem(
                    item2,
                    4,
                    replyTo
                )
            }, timeout)
        val summary3 = reply3.toCompletableFuture()[3, TimeUnit.SECONDS]
        Assert.assertEquals(1, summary3.getItems().size.toLong())
        Assert.assertEquals(4L, summary3.getItems()[item2]!!.toLong())

        probe.awaitAssert<Any?>(
            Duration.ofSeconds(10)
        ) {
            val item2Popularity =
                itemPopularityRepository!!.findById(item2)
            Assert.assertTrue(item2Popularity!!.isPresent)
            Assert.assertEquals((5 + 4).toLong(), item2Popularity.get().count)
            null
        }
    }

    @Test
    @Throws(Exception::class)
    fun safelyUpdatePopularityCount() {
        val sharding = ClusterSharding.get(system)

        val item = "concurrent-item"
        val cartCount = 30
        val itemCount = 1
        val timeout = Duration.ofSeconds(30)

        // Given `item1` is already on the popularity projection DB...
        val rep1 =
            sharding
                .entityRefFor(getEntityKey(), "concurrent-cart0")
                .askWithStatus({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                    AddItem(
                        item,
                        itemCount,
                        replyTo
                    )
                }, timeout)

        val probe = testKit().createTestProbe<Any>()
        probe.awaitAssert<Any?> {
            val item1Popularity =
                itemPopularityRepository!!.findById(item)
            Assert.assertTrue(item1Popularity!!.isPresent)
            Assert.assertEquals(itemCount.toLong(), item1Popularity.get().count)
            null
        }

        // ... when 29 concurrent carts add `item1`...
        for (i in 1 until cartCount) {
            sharding
                .entityRefFor(getEntityKey(), "concurrent-cart$i")
                .askWithStatus(
                    { replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> ->
                        AddItem(
                            item,
                            itemCount,
                            replyTo
                        )
                    }, timeout
                )
        }

        // ... then the popularity count is 30
        probe.awaitAssert<Any?>(
            timeout
        ) {
            val item1Popularity =
                itemPopularityRepository!!.findById(item)
            Assert.assertTrue(item1Popularity!!.isPresent)
            Assert.assertEquals((cartCount * itemCount).toLong(), item1Popularity.get().count)
            null
        }
    }

    companion object {
        private fun config(): Config {
            return ConfigFactory.load("item-popularity-integration-test.conf")
        }

        @ClassRule
        @JvmStatic
        fun testKit(): TestKitJunitResource = TestKitJunitResource(config())

        private val system: ActorSystem<*> = testKit().system()
        private var itemPopularityRepository: ItemPopularityRepository? = null

        @BeforeClass
        @JvmStatic
        fun beforeClass() {
            val springContext = applicationContext(system)
            itemPopularityRepository = springContext.getBean(
                ItemPopularityRepository::class.java
            )
            val transactionManager = springContext.getBean(
                JpaTransactionManager::class.java
            )
            // create schemas
            createTables(transactionManager, system)

            init(system)

            init(system, transactionManager, itemPopularityRepository!!)

            // form a single node cluster and make sure that completes before running the test
            val node = Cluster.get(system)
            node.manager().tell(Join.create(node.selfMember().address()))

            // let the node join and become Up
            val probe = testKit().createTestProbe<Any>()
            probe.awaitAssert<Any?> {
                Assert.assertEquals(MemberStatus.up(), node.selfMember().status())
                null
            }
        }
    }
}
