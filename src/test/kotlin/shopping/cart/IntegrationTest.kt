package shopping.cart

import akka.Done
import akka.actor.CoordinatedShutdown
import akka.actor.testkit.typed.javadsl.ActorTestKit
import akka.actor.testkit.typed.javadsl.TestProbe
import akka.actor.typed.ActorSystem
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.typed.Cluster
import akka.grpc.GrpcClientSettings
import akka.japi.function.Creator
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions
import akka.testkit.SocketUtil
import com.google.protobuf.CodedInputStream
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.orm.jpa.JpaTransactionManager
import scala.jdk.CollectionConverters
import shopping.cart.CreateTableTestUtils.createTables
import shopping.cart.Main.init
import shopping.cart.proto.AddItemRequest
import shopping.cart.proto.CheckedOut
import shopping.cart.proto.CheckoutRequest
import shopping.cart.proto.GetItemPopularityRequest
import shopping.cart.proto.ItemAdded
import shopping.cart.proto.ItemQuantityAdjusted
import shopping.cart.proto.ItemRemoved
import shopping.cart.proto.ShoppingCartService
import shopping.cart.proto.ShoppingCartServiceClient
import shopping.cart.proto.UpdateItemRequest
import shopping.cart.repository.SpringIntegration.applicationContext
import shopping.order.proto.OrderRequest
import shopping.order.proto.OrderResponse
import shopping.order.proto.ShoppingOrderService
import java.net.InetSocketAddress
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.function.*
import java.util.stream.*


class IntegrationTest {
    private class TestNodeFixture(grcpPort: Int, managementPorts: List<Int>, managementPortIndex: Int) {
        val testKit: ActorTestKit
        val system: ActorSystem<*>
        private val clientSettings: GrpcClientSettings
        private var client: ShoppingCartServiceClient? = null

        init {
            testKit =
                ActorTestKit.create(
                    "IntegrationTest",
                    nodeConfig(grcpPort, managementPorts, managementPortIndex)
                        .withFallback(sharedConfig())
                )
            system = testKit.system()
            clientSettings =
                GrpcClientSettings.connectToServiceAt("127.0.0.1", grcpPort, system).withTls(false)
        }

        fun getClient(): ShoppingCartService? {
            if (client == null) {
                client = ShoppingCartServiceClient.create(clientSettings, system)
                CoordinatedShutdown.get(system)
                    .addTask(
                        CoordinatedShutdown.PhaseBeforeServiceUnbind(),
                        "close-test-client-for-grpc",
                        Supplier { client!!.close() })
            }
            return client
        }
    }

    @Test
    @Throws(Exception::class)
    fun updateAndProjectFromDifferentNodesViaGrpc() {
        // add from client1
        val response1 =
            testNode1!!
                .getClient()!!
                .addItem(
                    AddItemRequest.newBuilder()
                        .setCartId("cart-1")
                        .setItemId("foo")
                        .setQuantity(42)
                        .build()
                )
        val updatedCart1 = response1.toCompletableFuture()[requestTimeout.seconds, TimeUnit.SECONDS]
        Assert.assertEquals("foo", updatedCart1.getItems(0).itemId)
        Assert.assertEquals(42, updatedCart1.getItems(0).quantity.toLong())

        // first may take longer time
        val published1 =
            kafkaTopicProbe!!.expectMessageClass(ItemAdded::class.java, Duration.ofSeconds(20))
        Assert.assertEquals("cart-1", published1.cartId)
        Assert.assertEquals("foo", published1.itemId)
        Assert.assertEquals(42, published1.quantity.toLong())

        // add from client2
        val response2 =
            testNode2!!
                .getClient()!!
                .addItem(
                    AddItemRequest.newBuilder()
                        .setCartId("cart-2")
                        .setItemId("bar")
                        .setQuantity(17)
                        .build()
                )
        val updatedCart2 = response2.toCompletableFuture()[requestTimeout.seconds, TimeUnit.SECONDS]
        Assert.assertEquals("bar", updatedCart2.getItems(0).itemId)
        Assert.assertEquals(17, updatedCart2.getItems(0).quantity.toLong())

        // update from client2
        val response3 =
            testNode2!!
                .getClient()!!
                .updateItem(
                    UpdateItemRequest.newBuilder()
                        .setCartId("cart-2")
                        .setItemId("bar")
                        .setQuantity(18)
                        .build()
                )
        val updatedCart3 = response3.toCompletableFuture()[requestTimeout.seconds, TimeUnit.SECONDS]
        Assert.assertEquals("bar", updatedCart3.getItems(0).itemId)
        Assert.assertEquals(18, updatedCart3.getItems(0).quantity.toLong())

        // ItemPopularityProjection has consumed the events and updated db
        val testProbe = testNode1!!.testKit.createTestProbe<Any>()
        testProbe.awaitAssert<Any?>(
            {
                Assert.assertEquals(
                    42,
                    testNode1!!
                        .getClient()!!
                        .getItemPopularity(GetItemPopularityRequest.newBuilder().setItemId("foo").build())
                        .toCompletableFuture()
                        .get(
                            requestTimeout.getSeconds(),
                            TimeUnit.SECONDS
                        )
                        .getPopularityCount()
                )
                Assert.assertEquals(
                    18,
                    testNode1!!
                        .getClient()!!
                        .getItemPopularity(GetItemPopularityRequest.newBuilder().setItemId("bar").build())
                        .toCompletableFuture()
                        .get(
                            requestTimeout.getSeconds(),
                            TimeUnit.SECONDS
                        )
                        .getPopularityCount()
                )
                null
            })

        val published2 = kafkaTopicProbe!!.expectMessageClass(
            ItemAdded::class.java
        )
        Assert.assertEquals("cart-2", published2.cartId)
        Assert.assertEquals("bar", published2.itemId)
        Assert.assertEquals(17, published2.quantity.toLong())

        val published3 =
            kafkaTopicProbe!!.expectMessageClass(ItemQuantityAdjusted::class.java)
        Assert.assertEquals("cart-2", published3.cartId)
        Assert.assertEquals("bar", published3.itemId)
        Assert.assertEquals(18, published3.quantity.toLong())

        val response4 =
            testNode2!!.getClient()!!.checkout(CheckoutRequest.newBuilder().setCartId("cart-2").build())
        val cart4 = response4.toCompletableFuture()[requestTimeout.seconds, TimeUnit.SECONDS]
        Assert.assertTrue(cart4.checkedOut)

        val orderRequest = orderServiceProbe!!.expectMessageClass(
            OrderRequest::class.java
        )
        Assert.assertEquals("cart-2", orderRequest.cartId)
        Assert.assertEquals("bar", orderRequest.getItems(0).itemId)
        Assert.assertEquals(18, orderRequest.getItems(0).quantity.toLong())

        val published4 = kafkaTopicProbe!!.expectMessageClass(
            CheckedOut::class.java
        )
        Assert.assertEquals("cart-2", published4.cartId)
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(IntegrationTest::class.java)

        private fun sharedConfig(): Config {
            return ConfigFactory.load("integration-test.conf")
        }

        private fun nodeConfig(
            grcpPort: Int, managementPorts: List<Int>, managementPortIndex: Int
        ): Config {
            return ConfigFactory.parseString(
                "shopping-cart-service.grpc.port = "
                        + grcpPort
                        + "\n"
                        + "akka.management.http.port = "
                        + managementPorts[managementPortIndex]
                        + "\n"
                        + "akka.discovery.config.services.shopping-cart-service.endpoints = [\n"
                        + "  { host = \"127.0.0.1\", port = "
                        + managementPorts[0]
                        + "},\n"
                        + "  { host = \"127.0.0.1\", port = "
                        + managementPorts[1]
                        + "},\n"
                        + "  { host = \"127.0.0.1\", port = "
                        + managementPorts[2]
                        + "},\n"
                        + "]"
            )
        }

        private fun initializeKafkaTopicProbe(
            system: ActorSystem<*>, kafkaTopicProbe: TestProbe<Any>?
        ) {
            val topic = system.settings().config().getString("shopping-cart-service.kafka.topic")
            val groupId = UUID.randomUUID().toString()
            val consumerSettings =
                ConsumerSettings.create(system, StringDeserializer(), ByteArrayDeserializer())
                    .withBootstrapServers("localhost:9092") // provided by Docker compose
                    .withGroupId(groupId)
            akka.kafka.javadsl.Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
                .map(
                    { record: ConsumerRecord<String, ByteArray> ->
                        val bytes: ByteArray = record.value()
                        val x: com.google.protobuf.Any = com.google.protobuf.Any.parseFrom(bytes)
                        val typeUrl: String = x.getTypeUrl()
                        val inputBytes: CodedInputStream = x.getValue().newCodedInput()
                        val event: Any
                        when (typeUrl) {
                            "shopping-cart-service/shoppingcart.ItemAdded" -> event =
                                ItemAdded.parseFrom(inputBytes)

                            "shopping-cart-service/shoppingcart.ItemQuantityAdjusted" -> event =
                                ItemQuantityAdjusted.parseFrom(inputBytes)

                            "shopping-cart-service/shoppingcart.ItemRemoved" -> event =
                                ItemRemoved.parseFrom(inputBytes)

                            "shopping-cart-service/shoppingcart.CheckedOut" -> event =
                                CheckedOut.parseFrom(inputBytes)

                            else -> throw IllegalArgumentException("Unknown record type [" + typeUrl + "]")
                        }
                        event
                    })
                .runForeach({ event: Any ->
                    kafkaTopicProbe!!.getRef().tell(event)
                }, system)
                .whenComplete(
                    { ok: Done?, ex: Throwable? ->
                        if (ex != null) logger.error("Test consumer of " + topic + " failed", ex)
                    })
        }

        private var testNode1: TestNodeFixture? = null
        private var testNode2: TestNodeFixture? = null
        private var testNode3: TestNodeFixture? = null
        private var systems: List<ActorSystem<*>>? = null
        private var kafkaTopicProbe: TestProbe<Any>? = null
        private var orderServiceProbe: TestProbe<OrderRequest>? = null
        private var testOrderService: ShoppingOrderService? = null
        private val requestTimeout: Duration = Duration.ofSeconds(10)

        @BeforeClass
        @JvmStatic
        fun setup() {
            val inetSocketAddresses =
                CollectionConverters.SeqHasAsJava(
                    SocketUtil.temporaryServerAddresses(6, "127.0.0.1", false)
                )
                    .asJava()
            val grpcPorts =
                inetSocketAddresses.subList(0, 3).stream()
                    .map({ obj: InetSocketAddress -> obj.getPort() })
                    .collect(Collectors.toList())
            val managementPorts =
                inetSocketAddresses.subList(3, 6).stream()
                    .map({ obj: InetSocketAddress -> obj.getPort() })
                    .collect(Collectors.toList())
            testNode1 = TestNodeFixture(grpcPorts[0], managementPorts, 0)
            testNode2 = TestNodeFixture(grpcPorts[1], managementPorts, 1)
            testNode3 = TestNodeFixture(grpcPorts[2], managementPorts, 2)
            systems = Arrays.asList(testNode1!!.system, testNode2!!.system, testNode3!!.system)

            val springContext = applicationContext(testNode1!!.system)
            val transactionManager = springContext.getBean(
                JpaTransactionManager::class.java
            )
            // create schemas
            createTables(transactionManager, testNode1!!.system)

            kafkaTopicProbe = testNode1!!.testKit.createTestProbe()

            orderServiceProbe = testNode1!!.testKit.createTestProbe()
            // stub of the ShoppingOrderService
            testOrderService =
                ShoppingOrderService { `in` ->
                    orderServiceProbe!!.getRef().tell(`in`)
                    CompletableFuture.completedFuture(
                        OrderResponse.newBuilder().setOk(true).build()
                    )
                }

            init(testNode1!!.testKit.system(), testOrderService!!)
            init(testNode2!!.testKit.system(), testOrderService!!)
            init(testNode3!!.testKit.system(), testOrderService!!)

            // wait for all nodes to have joined the cluster, become up and see all other nodes as up
            val upProbe = testNode1!!.testKit.createTestProbe<Any>()
            systems!!.forEach(
                java.util.function.Consumer { system: ActorSystem<*>? ->
                    upProbe.awaitAssert(
                        Duration.ofSeconds(15),
                        Creator<Any?> {
                            val cluster: Cluster = Cluster.get(system)
                            Assert.assertEquals(MemberStatus.up(), cluster.selfMember().status())
                            cluster
                                .state()
                                .getMembers()
                                .iterator()
                                .forEachRemaining(java.util.function.Consumer { member: Member ->
                                    Assert.assertEquals(
                                        MemberStatus.up(),
                                        member.status()
                                    )
                                })
                            null
                        })
                })

            initializeKafkaTopicProbe(testNode1!!.system, kafkaTopicProbe)
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            testNode3!!.testKit.shutdownTestKit()
            testNode2!!.testKit.shutdownTestKit()
            testNode1!!.testKit.shutdownTestKit()
        }
    }
}
