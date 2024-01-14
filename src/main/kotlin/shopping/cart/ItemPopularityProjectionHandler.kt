package shopping.cart

import akka.projection.eventsourced.EventEnvelope
import akka.projection.jdbc.javadsl.JdbcHandler
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shopping.cart.repository.HibernateJdbcSession
import shopping.cart.repository.ItemPopularityRepository


internal class ItemPopularityProjectionHandler(private val tag: String, private val repo: ItemPopularityRepository) :
    JdbcHandler<EventEnvelope<ShoppingCart.Event>, HibernateJdbcSession>() {
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private fun findOrNew(itemId: String): ItemPopularity {
        return repo.findById(itemId)!!.orElseGet {
            ItemPopularity(
                itemId,
                0L,
                0
            )
        }
    }

    override fun process(session: HibernateJdbcSession?, envelope: EventEnvelope<ShoppingCart.Event>) {
        val event = envelope.event()

        if (event is ShoppingCart.ItemAdded) {
            val added = event
            val itemId = added.itemId

            val existingItemPop = findOrNew(itemId)
            val updatedItemPop = existingItemPop.changeCount(added.quantity.toLong())
            repo.save(updatedItemPop)

            logger.info(
                "ItemPopularityProjectionHandler({}) item popularity for '{}': [{}]",
                this.tag,
                itemId,
                updatedItemPop.count
            )
        } else if (event is ShoppingCart.ItemQuantityAdjusted) {
            val adjusted = event
            val itemId = adjusted.itemId

            val existingItemPop = findOrNew(itemId)
            val updatedItemPop =
                existingItemPop.changeCount((adjusted.newQuantity - adjusted.oldQuantity).toLong())
            repo.save(updatedItemPop)
        } else if (event is ShoppingCart.ItemRemoved) {
            val removed = event
            val itemId = removed.itemId

            val existingItemPop = findOrNew(itemId)
            val updatedItemPop = existingItemPop.changeCount(-removed.oldQuantity.toLong())
            repo.save(updatedItemPop)
        } else {
            // skip all other events, such as `CheckedOut`
        }
    }
}

