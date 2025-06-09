use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};

use std::fmt::Write;

use clap::{App, Arg};
use log::{info, error, debug, warn};

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::message::{OwnedHeaders, Header, Headers};
use rdkafka::{ClientContext};
use rdkafka::consumer::{Consumer, StreamConsumer, Rebalance, BaseConsumer, ConsumerContext, CommitMode};
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
    http::StatusCode,
    routing::post,
    Router,
};

use antithesis_sdk::prelude::*;

use serde::{Serialize, Deserialize};
use serde_json::Value;
use serde_json::json;
use serde_postgres::de::from_row;

use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use tokio_postgres::{Client, NoTls, Error, Row};
use tokio_postgres::types::ToSql;

use std::thread::sleep;
use chrono::NaiveDateTime;

use postgres_types::{Type, IsNull};
use anyhow::{Result, Context};


use chrono;

#[derive(Debug)]
struct Postgres {
    client: Client
}

impl Postgres {
    pub async fn new(connection_string: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
        tokio::spawn(connection); // Spawn the connection as a background task
        Ok(Postgres { client })
    }

    async fn safe_write_consumed(&self, b_a_data: (BankAccount, NaiveDateTime), kafka_mode: &KafkaConsumers) {
        let (b_a, current_timestamp) = b_a_data;
        let update = "
            UPDATE producer
            SET 
                consumed_timestamp = $1,
                consumer_type = $2,
                consumed_count = consumed_count + 1
            WHERE id = $3;
        ";
        loop {
            // this performs update and returns number of rows updated. Not super useful for us, so we don't capture this number
            let result = self.client
                .execute(update, &[&current_timestamp, &kafka_mode.get_route(), &(b_a.id)]) // Assuming id is SERIAL (i32 in DB)
                .await;
            match result {
                Ok(d) => {
                    if d > 0 {
                        println!("postgres: Bank Account ID: {:?}, Updated timestamp: {}, Updated route: {}, result: d: {}", &b_a.id, &current_timestamp, &kafka_mode.get_route(), &d);
                        assert_sometimes!(true, "Recorded data consumed from Kafka to state-tracker", &json!({"result": b_a}));
                        break;
                    } else {
                        eprintln!("postgres: I guess we updated nothing...: {}", &d);
                        assert_unreachable!("Recorded 0 data consumed from Kafka to state-tracker with no errors", &json!({"result": "none"}));
                        sleep(Duration::from_secs(1)); // Avoid tight retry loop
                    }            
                },
                Err(e) => {
                    eprintln!("postgres: Failed to update timestamp: {}", e);
                    assert_sometimes!(false, "Recorded data consumed from Kafka to state-tracker", &json!({"error": format!("Failed to update timestamp {}", e)}));
                    sleep(Duration::from_secs(1)); // Avoid tight retry loop
                }
            }
        }
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
    ExactlyOncePassThroughConsumer,
    AtleastOncePassThroughConsumer
}

impl KafkaConsumers {
    fn get_route(&self) -> &'static str {
        match self {
            KafkaConsumers::ExactlyOncePassThroughConsumer => "/exactly_once_pass_through_consumer",
            KafkaConsumers::AtleastOncePassThroughConsumer => "/atleast_once_pass_through_consumer"
        }
    }
}

struct KafkaConsumer {
    kind: KafkaConsumers,
    consumer: Option<LoggingConsumer>
}


impl KafkaConsumer {
    fn create_config(&mut self, brokers: &Vec<&str>) {
        let config = match self.kind {
            KafkaConsumers::AtleastOncePassThroughConsumer => {
                let c = ClientConfig::new()
                    .set("group.id", "1")
                    .set("bootstrap.servers", brokers.join(","))
                    .set("enable.partition.eof", "false")
                    .set("session.timeout.ms", "30000")
                    .set("enable.auto.commit", "true")
                    //.set("statistics.interval.ms", "30000")
                    .set("auto.offset.reset", "earliest")
                    .set_log_level(RDKafkaLogLevel::Debug)
                    .create_with_context(CustomContext)
                    .expect("Consumer creation failed");
                println!("kafka: creating atleast once kafka consumer");
                assert_reachable!("Created \'Atleast Once\' passthrough Kafka consumer", &json!({}));
                c
            },
            KafkaConsumers::ExactlyOncePassThroughConsumer => {
                let c = ClientConfig::new()
                    .set("group.id", "2")
                    .set("bootstrap.servers", brokers.join(","))
                    .set("enable.partition.eof", "false")
                    .set("session.timeout.ms", "30000")
                    .set("isolation.level", "read_committed")
                    .set("enable.auto.commit", "false")        // we have to commit offset ourself
                    .set("auto.offset.reset", "earliest")
                    .set_log_level(RDKafkaLogLevel::Debug)
                    .create_with_context(CustomContext)
                    .expect("Consumer creation failed");
                println!("kafka: creating exactly once kafka consumer");
                assert_reachable!("Created \'Exactly Once\' passthrough Kafka consumer", &json!({}));
                c
            }
        };
        self.consumer = Some(config);
    }

    fn safe_subscribe(&mut self, topic: &str) {
        loop {
            let res = self.consumer.as_ref().unwrap().subscribe(&[&topic]);
            match res {
                Ok(_) => {
                    println!("kafka: subscribe to topic {}", &topic);
                    assert_sometimes!(false, "Consumer subscribed to topic", &json!({"result": format!("none")}));
                    break
                },
                Err(e) => {
                    eprintln!("kafka: Can't subscribe to specified topics: {}", e);
                    assert_sometimes!(false, "Consumer subscribed to topic", &json!({"error": format!("Can't subscribe to specified topics: {}", e)}));
                    sleep(Duration::from_secs(1)); // Avoid tight retry loop
                }
            }
        }
    }

    async fn safe_get_consumed(&mut self) -> Result<(BankAccount, NaiveDateTime)> {
        let message = self.consumer.as_ref().unwrap().recv().await
            .map_err(|e| {
                assert_sometimes!(false, "Consumer failed to receive message", &json!({ "error": format!("{:?}", e) }));
                e
            })?;

        assert_sometimes!(true, "Consumer consumed data", &json!({ "value": format!("{:?}", message) }));

        let payload_str = match message.payload_view::<str>() {
            Some(Ok(s)) => {
                println!("kafka: consumed message has correct string decoding");
                assert_sometimes!(true, "Consumer's consumed message has correct string decoding", &json!({ "value": s }));
                s
            },
            Some(Err(e)) => {
                eprintln!("kafka: consumed message has incorrect string decoding");
                assert_sometimes!(false, "Consumer's consumed message failed to decode payload", &json!({ "error": format!("{:?}", e) }));
                return Err(e.into());
            },
            None => {
                eprintln!("kafka: consumed message has no payload");
                assert_sometimes!(false, "Consumer's consumed message payload is None", &json!({ "error": "none" }));
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Kafka payload is None").into());
            }
        };
        
        match self.kind {
            KafkaConsumers::ExactlyOncePassThroughConsumer => {
                self.consumer.as_ref().unwrap().commit_message(&message, CommitMode::Sync)
                    .map_err(|e| {
                        println!("kafka: failed to commit message offset");
                        assert_sometimes!(false, "Consumer failed to commit offset", &json!({ "error": format!("{:?}", e) }));
                        e
                    })?;
            },
            _ => {
                println!("kafka: not committing, letting auto commit take care of it");
            } 
        }
        

        let current_timestamp = chrono::Utc::now().naive_utc(); 

        let b_a: BankAccount = serde_json::from_str(payload_str.trim())
            .map_err(|e| {
                println!("kafka: consumed message failed to deserialize: {:?}", e);
                assert_sometimes!(false, "Consumer's consumed message failed to deserialize to BankAccount", &json!({ "error": format!("{:?}", e) }));
                anyhow::Error::from(e)
                //e.into() // convert to anyhow::Error
            })?;

        println!("kafka: consumed message deserializable to BankAccount: {:?}", b_a);
        assert_sometimes!(false, "Consumer's consumed message deserialized to BankAccount", &json!({ "result": format!("{:?}", b_a) }));

        Ok((b_a, current_timestamp))
    }

    // async fn safe_get_consumed(&mut self) -> Result<(BankAccount, NaiveDateTime), Error> {
    //     match self.consumer.as_ref().unwrap().recv().await {
    //         Ok(message) => {
    //             assert_sometimes!(true, "Consumer consumed data", &json!({"value": format!("{:?}", message)}));
    //             if let Some(payload) = message.payload_view::<str>() {
    //                 match payload {
    //                     Ok(text) => {
    //                         let current_timestamp = chrono::Utc::now().naive_utc();
    //                         println!("kafka: Consumed message has correct string decoding");
    //                         assert_sometimes!(true, "Consumer's consumed message has correct string decoding", &json!({"value": text}));
    //                         let b_a = serde_json::from_str::<BankAccount>(text.trim())
    //                             .inspect_err(|e| {
    //                                 eprintln!("kafka: Can't deserialize BankAccount from producer");
    //                                 assert_unreachable!("Consumer failed to deserialize BankAccount from JSON", &json!({"error": format!("{:?}", e)}));
    //                             })?
    //                         println!("kafka: Received deserializable message: {:?}", b_a);
    //                         Ok((b_a, current_timestamp))
    //                     },
    //                     Err(e) => {
    //                         eprintln!("kafka: Failed to decode message payload: {}", e);
    //                         assert_sometimes!(false, "Consumer's consumed message has correct string decoding", &json!({"error": format!("{:?}", e)}));
    //                         Err(e)
    //                     },
    //                 }
    //             } 
    //         }
    //         Err(e) => {
    //             eprintln!("kafka: Error while consuming: {}", e);
    //             assert_sometimes!(false, "Consumer consumed data", &json!({"error": format!("{:?}", e)}));
    //             Err(e)
    //         }
    //     }
    // }
    
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
    // Should really put this in timeout to force this to stop holding the lock in handle
    // Right now we are just relying on multiple producers to fire to save us
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
    Extension(kafka_mode): Extension<KafkaConsumers>
) {
    let brokers = &state.brokers;
    println!("kafka: waiting for lock, previous thred needs to let go");
    let mut consumers = state.consumers.lock().await;
    let consumer = consumers
        .entry(kafka_mode.get_route().to_string())
        .or_insert_with(|| {
            let mut kc = KafkaConsumer {kind: kafka_mode.clone(), consumer: None};
            kc.create_config(brokers);
            kc
        });
     
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
        consumer.safe_subscribe(&topic);
        let mut count = 0;
        while count < num_records {
            let b_a_data = consumer.safe_get_consumed().await;
            match b_a_data {
                Ok(b_a_data) => {
                    pg_client.safe_write_consumed(b_a_data, &kafka_mode).await; 
                }
                Err(e) => {
                    eprintln!("kafka: could not consume data: {:?}", e);
                    sleep(Duration::from_secs(1)); // Avoid tight retry loop
                }
            }
            count += 1; 
        } 
    }

}


#[derive(Clone)]
struct AppState {
    consumers: Arc<Mutex<HashMap<String, KafkaConsumer>>>,
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
    let consumers: Arc<Mutex<HashMap<String, KafkaConsumer>>> = Arc::new(Mutex::new(HashMap::new()));
    let state = AppState { consumers, brokers, pg_client };
    // let app = create_nested_router(state.into());
    let app = create_router(state.into());
    println!("Starting Consumer");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server running at http:{:?}", listener);
    
    axum::serve(listener, app).await.unwrap(); 
}
