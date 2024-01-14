package shopping.cart.repository

import com.typesafe.config.Config
import com.zaxxer.hikari.HikariDataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.orm.jpa.JpaTransactionManager
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean
import org.springframework.orm.jpa.vendor.Database
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.EnableTransactionManagement
import java.util.*
import java.util.concurrent.*
import javax.sql.DataSource


/** Configure the necessary components required for integration with Akka Projections  */
@Configuration
@EnableJpaRepositories
@EnableTransactionManagement
open class SpringConfig(private val config: Config) {
    /**
     * Configures a [JpaTransactionManager] to be used by Akka Projections. The transaction
     * manager should be used to construct a [shopping.cart.repository.HibernateJdbcSession]
     * that is then used to configure the [akka.projection.jdbc.javadsl.JdbcProjection].
     */
    @Bean
    open fun transactionManager(): PlatformTransactionManager {
        return JpaTransactionManager(Objects.requireNonNull(entityManagerFactory().getObject()))
    }

    /** An EntityManager factory using the configured database connection settings.  */
    @Bean
    open fun entityManagerFactory(): LocalContainerEntityManagerFactoryBean {
        val vendorAdapter = HibernateJpaVendorAdapter()
        vendorAdapter.setDatabase(Database.POSTGRESQL)

        val factory = LocalContainerEntityManagerFactoryBean()
        factory.jpaVendorAdapter = vendorAdapter
        factory.setPackagesToScan("shopping.cart")
        // set the DataSource configured with settings in jdbc-connection-settings
        factory.dataSource = dataSource()
        // load additional properties from config jdbc-connection-settings.additional-properties
        factory.setJpaProperties(additionalProperties())

        return factory
    }

    /**
     * Returns a [DataSource] configured with the settings in `jdbc-connection-settings.driver`. See src/main/resources/persistence.conf and
     * src/main/resources/local-shared.conf
     */
    @Bean
    open fun dataSource(): DataSource {
        val dataSource = HikariDataSource()

        val jdbcConfig = jdbcConfig()

        // pool configuration
        dataSource.poolName = "read-side-connection-pool"
        dataSource.maximumPoolSize = jdbcConfig.getInt("connection-pool.max-pool-size")

        val timeout = jdbcConfig.getDuration("connection-pool.timeout", TimeUnit.MILLISECONDS)
        dataSource.connectionTimeout = timeout

        // database configuration
        dataSource.driverClassName = jdbcConfig.getString("driver")
        dataSource.jdbcUrl = jdbcConfig.getString("url")
        dataSource.username = jdbcConfig.getString("user")
        dataSource.password = jdbcConfig.getString("password")
        dataSource.isAutoCommit = false

        return dataSource
    }

    private fun jdbcConfig(): Config {
        return config.getConfig("jdbc-connection-settings")
    }

    /**
     * Additional JPA properties can be passed through config settings under `jdbc-connection-settings.additional-properties`. The properties must be defined as key/value
     * pairs of String/String.
     */
    fun additionalProperties(): Properties {
        val properties = Properties()

        val additionalProperties = jdbcConfig().getConfig("additional-properties")
        val entries = additionalProperties.entrySet()

        for ((key, value1) in entries) {
            val value = value1.unwrapped()
            if (value != null) properties.setProperty(key, value.toString())
        }

        return properties
    }

    @Bean
    open fun exceptionTranslation(): PersistenceExceptionTranslationPostProcessor {
        return PersistenceExceptionTranslationPostProcessor()
    }
}
