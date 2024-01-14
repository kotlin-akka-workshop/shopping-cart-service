package shopping.cart

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess
import akka.persistence.jdbc.query.javadsl.JdbcReadJournal
import akka.persistence.query.Offset
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.javadsl.EventSourcedProvider
import akka.projection.javadsl.AtLeastOnceProjection
import akka.projection.jdbc.javadsl.JdbcProjection
import org.springframework.orm.jpa.JpaTransactionManager
import shopping.cart.repository.HibernateJdbcSession
import shopping.order.proto.ShoppingOrderService
import java.util.*


object SendOrderProjection {
    fun init(
        system: ActorSystem<*>,
        transactionManager: JpaTransactionManager,
        orderService: ShoppingOrderService
    ) {
        ShardedDaemonProcess.get(system)
            .init(
                ProjectionBehavior.Command::class.java,
                "SendOrderProjection",
                ShoppingCart.TAGS.size,
                { index: Int ->
                    ProjectionBehavior.create(
                        createProjectionsFor(system, transactionManager, orderService, index)
                    )
                },
                ShardedDaemonProcessSettings.create(system),
                Optional.of(ProjectionBehavior.stopMessage())
            )
    }

    private fun createProjectionsFor(
        system: ActorSystem<*>,
        transactionManager: JpaTransactionManager,
        orderService: ShoppingOrderService,
        index: Int
    ): AtLeastOnceProjection<Offset, EventEnvelope<ShoppingCart.Event>> {
        val tag = ShoppingCart.TAGS[index]
        val sourceProvider =
            EventSourcedProvider.eventsByTag<ShoppingCart.Event>(system, JdbcReadJournal.Identifier(), tag)

        return JdbcProjection.atLeastOnceAsync(
            ProjectionId.of("SendOrderProjection", tag),
            sourceProvider,
            { HibernateJdbcSession(transactionManager) },
            {
                SendOrderProjectionHandler(
                    system,
                    orderService
                )
            },
            system
        )
    }
}
