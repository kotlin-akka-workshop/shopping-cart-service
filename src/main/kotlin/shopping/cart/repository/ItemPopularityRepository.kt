package shopping.cart.repository

import org.springframework.data.repository.Repository
import shopping.cart.ItemPopularity
import java.util.*


interface ItemPopularityRepository : Repository<ItemPopularity?, String?> {
    fun save(itemPopularity: ItemPopularity?): ItemPopularity?

    fun findById(id: String?): Optional<ItemPopularity>?
}
