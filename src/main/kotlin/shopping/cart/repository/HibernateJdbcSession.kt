package shopping.cart.repository

import akka.japi.function.Function
import akka.projection.jdbc.JdbcSession
import org.hibernate.Session
import org.hibernate.jdbc.ReturningWork
import org.springframework.orm.jpa.EntityManagerFactoryUtils
import org.springframework.orm.jpa.JpaTransactionManager
import org.springframework.transaction.TransactionStatus
import org.springframework.transaction.support.DefaultTransactionDefinition
import java.sql.Connection
import java.sql.SQLException
import java.util.*
import javax.persistence.EntityManager


/**
 * Hibernate based implementation of Akka Projection JdbcSession. This class is required when
 * building a JdbcProjection. It provides the means for the projection to start a transaction
 * whenever a new event envelope is to be delivered to the user defined projection handler.
 *
 *
 * The JdbcProjection will use the transaction manager to initiate a transaction to commit the
 * envelope offset. Then used in combination with JdbcProjection.exactlyOnce method, the user
 * handler code and the offset store operation participates on the same transaction.
 */
class HibernateJdbcSession(private val transactionManager: JpaTransactionManager) : DefaultTransactionDefinition(),
    JdbcSession {
    private val transactionStatus: TransactionStatus = transactionManager.getTransaction(
        this
    )

    fun entityManager(): EntityManager {
        return EntityManagerFactoryUtils.getTransactionalEntityManager(
            Objects.requireNonNull(transactionManager.entityManagerFactory)
        )!!
    }

    override fun <Result> withConnection(func: Function<Connection, Result>): Result {
        val entityManager = entityManager()
        val hibernateSession = (entityManager.delegate as Session)
        return hibernateSession.doReturningWork(
            ReturningWork { connection ->
                try {
                    return@ReturningWork func.apply(connection)
                } catch (e: SQLException) {
                    throw e
                } catch (e: Exception) {
                    throw SQLException(e)
                }
            })
    }

    override fun commit() {
        if (entityManager().isOpen) transactionManager.commit(transactionStatus)
    }

    override fun rollback() {
        if (entityManager().isOpen) transactionManager.rollback(transactionStatus)
    }

    override fun close() {
        entityManager().close()
    }
}
