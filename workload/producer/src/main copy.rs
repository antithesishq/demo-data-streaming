use std::time::Duration;
use std::fmt::Write;

use clap::{App, Arg};
use log::{info, error, debug};

use rdkafka::config::ClientConfig;
use rdkafka::message::{OwnedHeaders, Header, Headers};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use env_logger;

use reqwest;

use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;

use std::net::SocketAddr;
use rdkafka::producer::BaseProducer;


use axum::Router;
use axum::routing::post;
use axum::extract::State;
use axum::Json;

use serde_json::Value;

// Define shared state
#[derive(Clone)]
struct AppState {
    producers: Arc<Mutex<HashMap<String, FutureProducer>>>,
    producers_histories: Arc<Mutex<HashMap<String, Vec<Json<Value>>>>>,
    brokers: Vec<&'static str>
}

struct Payload {
    
}

#[tokio::main]
async fn main() {
    // Build the router with route handlers

    env_logger::init();

    let producers: Arc<Mutex<HashMap<String, FutureProducer>>> = Arc::new(Mutex::new(HashMap::new()));
    let producers_histories: Arc<Mutex<HashMap<String, Vec<Json<Value>>>>> = Arc::new(Mutex::new(HashMap::new()));
    let brokers = vec!["kafka-3:9092", "kafka-2:9093", "kafka-1:9092"];
    let faker_endpoints = vec!["batch", "single", "batch_sequential"];


    let state = AppState { producers, producers_histories, brokers };
    
    let app = Router::new()
        .route("/exactly_once", post(exactly_once))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server running at http:{:?}", listener);
    
    axum::serve(listener, app).await.unwrap();
    // rdkafka
}

// The payload is expected to be Json, so if you pass incorrect Json or plain text you will get a response back from Axum lik this:
// Expected request with `Content-Type: application/json`
async fn exactly_once(State(state): State<AppState>, Json(payload): Json<serde_json::Value>){
    let producer_type = "exactly_once".to_string();
    let mut producers = state.producers.lock().await;
    let mut producers_history = state.producers_histories.lock().await;
    let brokers = state.brokers;

    let topic = payload.get("batch").and_then(Value::as_str);
    let key = payload.get("key").and_then(Value::as_str);
    let value = payload
        .get("value")
        .and_then(|v| serde_json::to_string(v).ok());

    println!("Topic: {:?}", topic);
    println!("Key: {:?}", key);
    println!("Value: {:?}", value);
    if let (Some(key), Some(value), Some(topic)) = (key, value, topic) {
        // let mut data = OwnedHeaders::new().insert(Header {
        //     key,
        //     value: Some(&v),
        // });

        // // Log the header for debugging
        // println!("Generated header: {:?}", data);

        // for h in data.iter() {
        //     println!("Header Key: {}, Header Value: {:?}", h.key, h.value);
        // }

        let producer = producers
            .entry(producer_type)
            .or_insert(
                ClientConfig::new()
                .set("bootstrap.servers", brokers.join(","))
                .set("message.timeout.ms", "5000")
                .set("debug", "all")
                .set("retries", "3") // Set the number of retries, maybe 3 okay XD
                .set("acks", "1")    // Wait for leader acknowledgment only lol
                .create::<FutureProducer>()
                .expect("Oopsies at-least-once producer creation error")
            )
            .send(
                FutureRecord::to(topic)
                    .payload(&format!("Message {}", value))
                    .key(&format!("Key {}", key)),
                    //.headers(meta_data), add meta_data here if necessary 
                Duration::from_secs(0),
            )
            .await;
            // Continue with your logic here (e.g., storing history, producing messages, etc.)

            // WHAT THE CONSUMER SHOULD LOOK LIKE
            // let consumer = ClientConfig::new()
            // .set("bootstrap.servers", "kafka-3:9092,kafka-2:9093,kafka-1:9092")
            // .set("group.id", "at_least_once_group")
            // .set("enable.auto.commit", "true") // Automatically commit offsets
            // .set("auto.offset.reset", "earliest")
            // .create::<rdkafka::consumer::StreamConsumer>()
            // .expect("Failed to create at-least-once consumer");
    } else {
        error!("Missing required fields: key or value");
        // Handle the error case, e.g., returning an error response to the client
    }

    // println!("WOW: {:?}", data);
    // Print the headers manually

    // let producer = producers
    //     .entry(producer_type)
    //     // Maybe do more retries
    //     .or_insert(
    //         &ClientConfig::new()
    //             .set("bootstrap.servers", payload.brokers)
    //             .set("message.timeout.ms", "5000")
    //             .set("debug", "all")
    //             .create()
    //             .expect("exactly_once producer creation error"))
    //     .send(
    //         FutureRecord::to(payload.topic_name)
    //             .payload(&format!("Message {}", "hi"))
    //             .key(&format!("Key {}", "hi"))
    //             .headers(data),
    //         Duration::from_secs(0),
    //     )
    //     .await
    //     .and_then(|delivery| {
    //         info!("Message {} delivered to {:?}", i, delivery);
    //         let producers_history = producers_histories
    //             .entry(producer_type)
    //             .or_insert(
    //                 Vec::new(data)
    //             )
    //         // Save here with timestamp
    //     })
    //     .map_err(|e| {
    //         error!("Failed to deliver message {}: {:?}", i, e);
    //     });
}



// EXACTLY ONCE PRODUCER N CONSUMER
// let producer = ClientConfig::new()
//     .set("bootstrap.servers", "kafka-3:9092,kafka-2:9093,kafka-1:9092")
//     .set("message.timeout.ms", "5000") 
//     .set("acks", "all") // Wait for all replicas to acknowledge
//     .set("enable.idempotence", "true") // Enable idempotent producer
//     .set("transactional.id", "exactly_once_txn") // Enable transactions
//     .create::<FutureProducer>()
//     .expect("Failed to create exactly-once producer");


// let consumer = ClientConfig::new()
//     .set("bootstrap.servers", "kafka-3:9092,kafka-2:9093,kafka-1:9092")
//     .set("group.id", "exactly_once_group")
//     .set("isolation.level", "read_committed") // Only read committed messages
//     .create::<rdkafka::consumer::StreamConsumer>()
//     .expect("Failed to create exactly-once consumer");


// Api {
//     produce_atleast_once
//     produce_atmost_once
//     produce_exact_once
// }

// async fn produce(brokers: &str, topic_name: &str) {
//     let producer: &FutureProducer = &ClientConfig::new()
//         .set("bootstrap.servers", brokers)
//         .set("message.timeout.ms", "5000")
//         .set("debug", "all")
//         .set("retry", "3")
//         .create()
//         .expect("Producer creation error");

//     // Turn this into a service and have test composer script query faker and pipe that data into here
//     // Run Consumer in single partition mode to make this FIFO queue 
//     // Query Faker, batch sequential
//     // send the data one by one using Kafka producer
//     // persist Faker data in order

//     let futures = (0..5)
//         .map(|i| async move {
//             // The send operation on the topic returns a future, which will be
//             // completed once the result or failure from Kafka is received.

//             let delivery_status = producer
//                 .send(
//                     FutureRecord::to(topic_name)
//                         .payload(&format!("Message {}", i))
//                         .key(&format!("Key {}", i))
//                         .headers(OwnedHeaders::new().insert(Header {
//                             key: "header_key",
//                             value: Some("header_value"),
//                         })),
//                         // .headers(OwnedHeaders::new()
//                         //     .add("header_key", "header_value")
//                         // ),
//                     Duration::from_secs(0),
//                 )
//                 .await;
            
//             match delivery_status {
//                 Ok(delivery) => info!("Message {} delivered to {:?}, Save here with timestamp ", i, delivery),
//                 Err(ref e) => error!("Failed to deliver message {}: {:?}", i, e),
//             }

//             // This will be executed when the result is received.
//             info!("Delivery status for message {} received, {:?}", i, delivery_status);
//             delivery_status
//         })
//         .collect::<Vec<_>>();

//     // This loop will wait until all delivery statuses have been received.
//     for future in futures {
//         info!("Future completed. Result: {:?}", future.await);
//     }
// }


                // let mut payload = payload.unwrap();
                // let topic = payload.get("topic").and_then(Value::as_str).expect("topic is none");
                // let key = payload.get("key").and_then(Value::as_str).expect("key is none");
                // let key = format!("Key {}", key);
                // let value = payload
                //     .get("value")
                //     .and_then(|v| serde_json::to_string(v).ok())
                //     .expect("value is none");
                // let value = format!("Message {}", value);
                // let producer = producers
                //     .entry(mode.to_path().to_string())
                //     .or_insert(mode.create_config(brokers))
                //     .send(
                //         FutureRecord::to(topic)
                //             .payload(&value)
                //             .key(&key),
                //         Duration::from_secs(0),
                //     );