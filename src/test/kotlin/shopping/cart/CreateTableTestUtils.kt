package shopping.cart

import akka.actor.typed.ActorSystem
import akka.persistence.jdbc.testkit.javadsl.SchemaUtils
import akka.projection.jdbc.javadsl.JdbcProjection
import org.slf4j.LoggerFactory
import org.springframework.orm.jpa.JpaTransactionManager
import shopping.cart.repository.HibernateJdbcSession
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.*
import java.util.stream.*


object CreateTableTestUtils {
    /**
     * Test utility to create journal and projection tables for tests environment. JPA/Hibernate
     * tables are auto created (drop-and-create) using settings flag, see persistence-test.conf
     */
    fun createTables(transactionManager: JpaTransactionManager?, system: ActorSystem<*>) {
        // create schemas
        // ok to block here, main test thread

        SchemaUtils.dropIfExists(system).toCompletableFuture()[30, TimeUnit.SECONDS]
        SchemaUtils.createIfNotExists(system).toCompletableFuture()[30, TimeUnit.SECONDS]

        JdbcProjection.dropOffsetTableIfExists(
            { HibernateJdbcSession(transactionManager!!) }, system
        )
            .toCompletableFuture()[30, TimeUnit.SECONDS]
        JdbcProjection.createOffsetTableIfNotExists(
            { HibernateJdbcSession(transactionManager!!) }, system
        )
            .toCompletableFuture()[30, TimeUnit.SECONDS]

        dropCreateUserTables(system)

        LoggerFactory.getLogger(CreateTableTestUtils::class.java).info("Tables created")
    }

    /**
     * Drops and create user tables (if applicable)
     *
     *
     * This file is only created on step 4 (projection query). Calling this method has no effect on
     * previous steps.
     */
    private fun dropCreateUserTables(system: ActorSystem<*>) {
        val path = Paths.get("ddl-scripts/create_user_tables.sql")
        if (path.toFile().exists()) {
            SchemaUtils.applyScript("DROP TABLE IF EXISTS public.item_popularity;", system)
                .toCompletableFuture()[30, TimeUnit.SECONDS]

            val script = Files.lines(path, StandardCharsets.UTF_8).collect(Collectors.joining("\n"))
            SchemaUtils.applyScript(script, system).toCompletableFuture()[30, TimeUnit.SECONDS]
        }
    }
}
