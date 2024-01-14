package shopping.cart.repository

import akka.Done
import akka.actor.typed.ActorSystem
import com.typesafe.config.Config
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import java.util.function.*


/**
 * Provides an integration point for initializing a Spring [ApplicationContext] configured for
 * working with Akka Projections.
 */
object SpringIntegration {
    /**
     * Returns a Spring [ApplicationContext] configured to provide all the infrastructure
     * necessary for working with Akka Projections.
     */
    fun applicationContext(system: ActorSystem<*>): ApplicationContext {
        val config = system.settings().config()
        val context = AnnotationConfigApplicationContext()
        // register the Config as a bean, so it can be later injected into SpringConfig
        context.registerBean(
            Config::class.java,
            Supplier { config })
        context.register(SpringConfig::class.java)
        context.refresh()

        // Make sure the Spring context is closed when the actor system terminates
        system.whenTerminated.thenAccept { done: Done? -> context.close() }

        return context
    }
}
