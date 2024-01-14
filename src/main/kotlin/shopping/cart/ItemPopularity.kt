package shopping.cart

import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table
import javax.persistence.Version

@Entity
@Table(name = "item_popularity")
class ItemPopularity {
    // primary key
    @Id
    val itemId: String

    // optimistic locking
    @Version
    val version: Long?

    val count: Long

    constructor() {
        // null version means the entity is not on the DB
        this.version = null
        this.itemId = ""
        this.count = 0
    }

    constructor(itemId: String, version: Long?, count: Long) {
        this.itemId = itemId
        this.version = version
        this.count = count
    }

    fun changeCount(delta: Long): ItemPopularity {
        return ItemPopularity(itemId, version, count + delta)
    }
}
