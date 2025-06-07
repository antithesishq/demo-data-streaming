use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};

use std::fmt::Write;

use clap::{App, Arg};
use log::{info, error, debug, warn};

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::message::{OwnedHeaders, Header, Headers};
use rdkafka::{ClientContext};
use rdkafka::consumer::{Consumer, StreamConsumer, Rebalance, BaseConsumer, ConsumerContext};
use rdkafka::util::get_rdkafka_version;
use rdkafka::TopicPartitionList;
use rdkafka::error::KafkaResult;
use rdkafka::Message;

use env_logger;

use reqwest;

use tokio::sync::Mutex;
use tracing_subscriber::fmt::format;
use std::sync::Arc;
use std::collections::HashMap;
// use std::error::Error;

use std::net::SocketAddr;

use axum::{
    body::Body,
    extract::{State, Json, Extension, Path},
    middleware::{self, Next},
    response::{Response, Json as ResponseJson, IntoResponse},
    routing::post,
    Router,
    http::StatusCode
};

use antithesis_sdk::prelude::*;

use serde::{Serialize, Deserialize};
use serde_json::Value;
use serde_json::json;
use serde_postgres::de::from_row;

use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use tokio_postgres::{Client, NoTls, Error, Row};

use postgres_types::{Type, IsNull};


use chrono;

#[derive(Debug)]
struct Postgres {
    client: Client
}

impl Postgres {
    pub async fn new(connection_string: &str) -> Result<Self, Error> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
        tokio::spawn(connection); // Spawn the connection as a background task
        Ok(Postgres { client })
    }

    async fn write_consumed(&self, b_a: &BankAccount, mode: &KafkaConsumers) -> Result<(), Error> {
        // Instead of update, you should write the entire BankAccount to a new consumers table
        // then the validator should check if both bank accounts referenced by the same ids in the consumer and producer are the same
        // OR
        // Validator should look at the latest produced message and see that all prior messages were produced
        let update = "
            UPDATE producer
            SET 
                consumed_timestamp = $1,
                consumer_type = $2,
                consumed_count = consumed_count + 1
            WHERE id = $3;
        ";
        let current_timestamp = chrono::Utc::now().naive_utc();
        // this performs update and returns number of rows updated. Not super useful for us, so we don't capture this number
        self.client
            .execute(update, &[&current_timestamp, &mode.get_route(), &(b_a.id)]) // Assuming id is SERIAL (i32 in DB)
            .await
            .inspect(|d| {
                if *d > 0 {
                    println!("postgres: b_a: {:?}, Updated timestamp: {}, Updated route: {}, d: {}", &b_a, &current_timestamp, &mode.get_route(), &d);
                    assert_sometimes!(true, "Recorded data consumed from Kafka to state-tracker", &json!({"result": b_a}));
                } else {
                    eprintln!("postgres: I guess we updated nothing...: {}", &d);
                }
            })
            .inspect_err(|e| {
                eprintln!("postgres: Failed to update timestamp: {}", e);
                assert_sometimes!(false, "Recorded data consumed from Kafka to state-tracker", &json!({"error": format!("Failed to update timestamp {}", e)}));
            })?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct BankAccount {
    id: i32,
    aba: String,
    iban: String,
    swift11: String,
    bank_country: String,
    produced_timestamp: Option<SystemTime>,
    producer_type: Option<String>,
    consumed_timestamp: Option<SystemTime>,
    consumer_type: Option<String>,
    consumed_count: i32
    // consumed: bool
}

impl BankAccount {
    fn from_row(row: &Row) -> Self {
        BankAccount {
            id: row.get("id"),
            aba: row.get("aba"),
            iban: row.get("iban"),
            swift11: row.get("swift11"),
            bank_country: row.get("bank_country"),
            produced_timestamp: row.try_get("produced_timestamp").ok(),
            producer_type: row.try_get("producer_type").ok(),
            consumed_timestamp: row.try_get("consumed_timestamp").ok(),
            consumer_type: row.try_get("consumer_type").ok(),
            consumed_count: row.get("consumed_count")
        }
    }
}

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }
}

// A type alias of custom consumer (for convenience).
type LoggingConsumer = StreamConsumer<CustomContext>;

#[derive(Clone, Debug, EnumIter)]
enum KafkaConsumers {
    PassThroughConsumer
}

impl KafkaConsumers {
    fn create_config(&self, brokers: &Vec<&str>) -> LoggingConsumer {
        match self {
            KafkaConsumers::PassThroughConsumer => {
                let c = ClientConfig::new()
                    .set("group.id", "my_group_id")
                    .set("bootstrap.servers", brokers.join(","))
                    .set("enable.partition.eof", "false")
                    .set("session.timeout.ms", "6000")
                    .set("enable.auto.commit", "true")
                    //.set("statistics.interval.ms", "30000")
                    .set("auto.offset.reset", "earliest")
                    .set_log_level(RDKafkaLogLevel::Debug)
                    .create_with_context(CustomContext)
                    .expect("Consumer creation failed");
                assert_reachable!("Created \'Pass Through\' Kafka consumer", &json!({}));
                c
            },
        }
    }

    fn get_route(&self) -> &'static str {
        match self {
            KafkaConsumers::PassThroughConsumer => "/pass_through_consumer",
        }
    }
}


fn create_router(state: Arc<AppState>) -> Router {
    let mut r = Router::new();
    for kafka_mode in KafkaConsumers::iter() {
        r = r.nest(
            kafka_mode.get_route(),
            create_nested_router(state.clone(), kafka_mode)
        )
    }
    r
}

fn create_nested_router(state: Arc<AppState>, mode: KafkaConsumers) -> Router{
     Router::new()
        .route("/:topic/:num_records", post(handle))
        // .route("/single/:topic", post(handle_single))
        // .route("/batch_sequential/:topic/:", post(handle_sequential_batch))
        .with_state(state) // Attach state to the router
        .layer(middleware::from_fn(move |req, next| {
            add_context_middleware(req, next, mode.clone())
        }))
       
}

async fn add_context_middleware(
    mut req: axum::http::Request<Body>,
    next: Next,
    context: KafkaConsumers, // Pass context explicitly
) -> Response {
    println!("middleware: adding context to request");
    req.extensions_mut().insert(context);
    match tokio::spawn(next.run(req)).await {
        Ok(resp) => resp,
        Err(e) => {
            eprintln!("middleware: task failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn handle(
    Path((topic, num_records)): Path<(String, i64)>,
    State(state): State<Arc<AppState>>,
    Extension(mode): Extension<KafkaConsumers>
) {
    let brokers = &state.brokers;

    let mut consumers = state.consumers.lock().await;
    let consumer = consumers
        .entry(mode.get_route().to_string())
        .or_insert_with(|| mode.create_config(brokers));
    let mut pg_client_guard = state.pg_client.lock().await;
    
    if pg_client_guard.is_none() {
        *pg_client_guard = Some(
            Postgres::new("host=state-tracker user=u password=p dbname=d ")
            .await
            .map_err(|e| {
                eprintln!("postgres: Failed to initialize Postgres {:?}", e);
                e
            })
            .unwrap()
        );
        assert_reachable!("Consumer created connection to state-tracker", &json!({}));
    }

    if let Some(pg_client) = &*pg_client_guard {
        consumer.subscribe(&[&topic])
            .map_err(|e| {
                eprintln!("kafka: Can't subscribe to specified topics: {}", e);
                assert_sometimes!(false, "Consumer subscribed to topic", &json!({"error": format!("Can't subscribe to specified topics: {}", e)}));
                e
            })
            .unwrap();
        
        println!("kafka: Subbed to topic");
        assert_sometimes!(true, "Consumer subscribed to topic", &json!({"value": topic}));

        let mut count = 0;
        while count < num_records {
            match consumer.recv().await {
                Ok(message) => {
                    assert_sometimes!(true, "Consumer consumed data", &json!({"value": format!("{:?}", message)}));
                    if let Some(payload) = message.payload_view::<str>() {
                        match payload {
                            Ok(text) => {
                                assert_sometimes!(true, "Consumer's consumed message has correct string decoding", &json!({"value": text}));
                                let b_a = serde_json::from_str::<BankAccount>(text.trim())
                                    .map_err(|e| {
                                        eprintln!("kafka: Can't serialize BankAccount from producer");
                                        assert_unreachable!("Consumer failed to serialize BankAccount to JSON", &json!({"error": format!("Failed to serialize BankAccount to JSON {:?}", e)}));
                                        e
                                    })
                                    .unwrap();
                                println!("kafka: Received message: {:?}", b_a);
                                loop {
                                    match pg_client.write_consumed(&b_a, &mode).await {
                                        Ok(_) => {
                                            println!("postgres: update complete");
                                            break; 
                                        },
                                        Err(e) => {
                                            eprintln!("postgres: Failed to send data to postgres {:?}", e);
                                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                                            println!("postgres: Going to retry  {:?}", e);
                                        }
                                    }
                                }
                            },
                            Err(e) => {
                                eprintln!("kafka: Failed to decode message payload: {}", e);
                                assert_sometimes!(false, "Consumer's consumed message has correct string decoding", &json!({"error": format!("Failed to decode message payload: {}", e)}));
                            },
                        }
                    }
                    count += 1;
                }
                Err(e) => {
                    eprintln!("kafka: Error while consuming: {}", e);
                    assert_sometimes!(false, "Consumer consumed data", &json!({"error": format!("Error while consuming: {}", e)}));

                }
            }
        }
    }

}


#[derive(Clone)]
struct AppState {
    consumers: Arc<Mutex<HashMap<String, LoggingConsumer>>>,
    brokers: Vec<&'static str>,
    pg_client: Arc<Mutex<Option<Postgres>>>
    // faker_endpoints: Vec<&'static str>
}

#[tokio::main]
async fn main() {
    antithesis_init();
    // env_logger::init();

    let brokers = vec!["kafka-3:9092", "kafka-2:9092", "kafka-1:9092"];
    let pg_client = Arc::new(Mutex::new(None));
    let consumers: Arc<Mutex<HashMap<String, LoggingConsumer>>> = Arc::new(Mutex::new(HashMap::new()));
    let state = AppState { consumers, brokers, pg_client };
    // let app = create_nested_router(state.into());
    let app = create_router(state.into());
    println!("Starting Consumer");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server running at http:{:?}", listener);
    
    axum::serve(listener, app).await.unwrap(); 
}