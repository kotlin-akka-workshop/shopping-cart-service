# Akka

This is translation from Java to Kotlin for the marvelous tutorial from Lightbend:

[Implementing Microservices with Akka](https://developer.lightbend.com/docs/akka-guide/microservices-tutorial/overview.html)

## Changes to the original Java code

[The original code](https://developer.lightbend.com/docs/akka-guide/microservices-tutorial/_attachments/6-shopping-cart-complete-java.zip)

- Migrate code from Java to Kotlin 1.9
- Simplify starting docker compose - no need to init the DB (it is done automatically)
- No need to install Maven (the wrapper is used)
- Upgraded version of Spring
- Upgraded version of some Maven plugins

***In progress***:
*The work is started to launch the whole cluster locally with docker compose instead of 
manually starting every service 3 times with different parameters.*

## Running the sample code

1. Start a local PostgresSQL server on default port 5432 and a Kafka broker on port 9092. 
The included `docker-compose.yml` starts everything required for running locally.

    ```shell
    docker compose up -d
    ```

2. Make sure you have compiled the project

    ```shell
    ./mvnw compile 
    ```

3. Start a first node:

    ```shell
    ./mvnw compile exec:exec -DAPP_CONFIG=local1.conf
    ```

4. (Optional) Start another node with different ports:

    ```shell
    ./mvnw compile exec:exec -DAPP_CONFIG=local2.conf
    ```

5. (Optional) More can be started:

    ```shell
    ./mvnw compile exec:exec -DAPP_CONFIG=local3.conf
    ```

6. Check for service readiness

    ```shell
    curl http://localhost:9101/ready
    ```

7. Try it with [grpcurl](https://github.com/fullstorydev/grpcurl):

    ```shell
    # add item to cart
    grpcurl -d '{"cartId":"cart1", "itemId":"socks", "quantity":3}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.AddItem
    
    # get cart
    grpcurl -d '{"cartId":"cart1"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.GetCart
    
    # update quantity of item
    grpcurl -d '{"cartId":"cart1", "itemId":"socks", "quantity":5}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.UpdateItem
    
    # check out cart
    grpcurl -d '{"cartId":"cart1"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.Checkout
    
    # get item popularity
    grpcurl -d '{"itemId":"socks"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.GetItemPopularity
    ```

    or same `grpcurl` commands to port 8102 to reach node 2.
