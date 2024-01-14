package shopping.cart

import akka.Done
import akka.actor.testkit.typed.javadsl.TestKitJunitResource
import akka.persistence.query.Offset
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
import akka.projection.javadsl.Handler
import akka.projection.testkit.javadsl.ProjectionTestKit
import akka.projection.testkit.javadsl.TestProjection
import akka.projection.testkit.javadsl.TestSourceProvider
import akka.stream.javadsl.Source
import org.junit.Assert
import org.junit.ClassRule
import org.junit.Test
import shopping.cart.repository.ItemPopularityRepository
import java.time.Instant
import java.util.*
import java.util.concurrent.*

class ItemPopularityProjectionTest {
    // stub out the db layer and simulate recording item count updates
    internal class TestItemPopularityRepository : ItemPopularityRepository {
        val itemPops: MutableMap<String?, ItemPopularity?> = HashMap()

        override fun save(itemPopularity: ItemPopularity): ItemPopularity {
            itemPops[itemPopularity.itemId] = itemPopularity
            return itemPopularity
        }

        override fun findById(id: String): Optional<ItemPopularity> {
            return Optional.ofNullable(itemPops[id])
        }
    }

    private fun createEnvelope(event: ShoppingCart.Event, seqNo: Long): EventEnvelope<ShoppingCart.Event> {
        return EventEnvelope(Offset.sequence(seqNo), "persistenceId", seqNo, event, 0L)
    }

    private fun toAsyncHandler(
        itemHandler: ItemPopularityProjectionHandler
    ): Handler<EventEnvelope<ShoppingCart.Event>> {
        return object : Handler<EventEnvelope<ShoppingCart.Event>>() {
            override fun process(eventEventEnvelope: EventEnvelope<ShoppingCart.Event>): CompletionStage<Done> {
                return CompletableFuture.supplyAsync {
                    itemHandler.process( // session = null is safe.
                        // The real handler never uses the session. The connection is provided to the repo
                        // by Spring itself
                        null, eventEventEnvelope
                    )
                    Done.getInstance()
                }
            }
        }
    }

    @Test
    fun itemPopularityUpdateUpdate() {
        val events =
            Source.from(
                Arrays.asList(
                    createEnvelope(ShoppingCart.ItemAdded("a7079", "bowling shoes", 1), 0L),
                    createEnvelope(
                        ShoppingCart.ItemQuantityAdjusted("a7079", "bowling shoes", 1, 2), 1L
                    ),
                    createEnvelope(
                        ShoppingCart.CheckedOut("a7079", Instant.parse("2020-01-01T12:00:00.00Z")),
                        2L
                    ),
                    createEnvelope(ShoppingCart.ItemAdded("0d12d", "akka t-shirt", 1), 3L),
                    createEnvelope(ShoppingCart.ItemAdded("0d12d", "skis", 1), 4L),
                    createEnvelope(ShoppingCart.ItemRemoved("0d12d", "skis", 1), 5L),
                    createEnvelope(
                        ShoppingCart.CheckedOut("0d12d", Instant.parse("2020-01-01T12:05:00.00Z")),
                        6L
                    )
                )
            )

        val repository = TestItemPopularityRepository()
        val projectionId = ProjectionId.of("item-popularity", "carts-0")

        val sourceProvider =
            TestSourceProvider.create(
                events
            ) { obj: EventEnvelope<ShoppingCart.Event> -> obj.offset() }

        val projection =
            TestProjection.create(
                projectionId,
                sourceProvider
            ) {
                toAsyncHandler(
                    ItemPopularityProjectionHandler("carts-0", repository)
                )
            }

        projectionTestKit.run(
            projection
        ) {
            Assert.assertEquals(3, repository.itemPops.size.toLong())
            Assert.assertEquals(2L, repository.itemPops["bowling shoes"]!!.count)
            Assert.assertEquals(1L, repository.itemPops["akka t-shirt"]!!.count)
            Assert.assertEquals(0L, repository.itemPops["skis"]!!.count)
        }
    }

    companion object {
        @ClassRule
        @JvmStatic
        fun testKit(): TestKitJunitResource = TestKitJunitResource()

        val projectionTestKit: ProjectionTestKit = ProjectionTestKit.create(
            testKit().system()
        )
    }
}