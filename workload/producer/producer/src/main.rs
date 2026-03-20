use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};

use std::fmt::Write;

use clap::{App, Arg};
use log::{debug, error, info, warn};

use rdkafka::config::ClientConfig;
use rdkafka::message::{OwnedHeaders, Header, Headers};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::producer::Producer;
use rdkafka::util::get_rdkafka_version;
use rdkafka::util::Timeout;
use rdkafka::error::KafkaError;

use env_logger;
use rand::Rng;
use anyhow::{anyhow, Result};

use reqwest;
//use reqwest::StatusCode;

use tokio::runtime::Handle;
use tokio::signal::unix::SignalKind;
use tokio::sync::Mutex;

use std::thread::sleep;

use std::sync::Arc;
use std::collections::HashMap;
// use std::error::Error;

use std::net::SocketAddr;
use rdkafka::producer::BaseProducer;

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

use postgres_types::{Type, IsNull};

use chrono;

// Define shared state
#[derive(Clone)]
struct AppState {
    producers: Arc<Mutex<HashMap<String, KafkaProducer>>>,
    brokers: Vec<&'static str>,
    pg_client: Arc<Mutex<Option<Postgres>>>
    // faker_endpoints: Vec<&'static str>
}

#[derive(Clone, Debug, EnumIter)]
enum KafkaProducers {
    ExactlyOnce,
    AtLeastOnce,
    AtLeastOnceBatch,
    ExactlyOnceBatch
}

impl KafkaProducers {
    fn get_route(&self) -> &'static str {
        match self {
            KafkaProducers::ExactlyOnce => "/exactly_once_single",
            KafkaProducers::ExactlyOnceBatch => "/exactly_once_batch",
            KafkaProducers::AtLeastOnce => "/atleast_once_single",
            KafkaProducers::AtLeastOnceBatch => "/atleast_once_batch"
        }
    }
}

struct KafkaProducer {
    kind: KafkaProducers,
    producer: Option<FutureProducer>
}



fn is_fatal_error(err: &KafkaError) -> bool {
    match err {
        KafkaError::Transaction(e) => {
            if e.is_fatal() {
                eprintln!("kafka: fatal error: {}", e);
                return true 
            } 
            eprintln!("kafka: not fatal error: {}", e);
            false
        },
        // KafkaError::MessageProduction(code)
        // | KafkaError::MessageConsumption(code)
        // | KafkaError::MessageConsumptionFatal(code)
        // | KafkaError::Flush(code)
        // | KafkaError::Commit(code)
        // | KafkaError::MetadataFetch(code)
        // | KafkaError::GroupListFetch(code)
        // | KafkaError::OffsetFetch(code)
        // | KafkaError::StoreOffset(code) => false, // Setting all these not fatal unless proven otherwise
        // | KafkaError::ConsumerCommit(code) => code.is_fatal()
        // KafkaError::Subscription(code_str) =>
        // | KafkaError::Seek(code) => probable not that bad
        e => {
            println!("kafka: non fatal error {}", e);
            false
        }
    }
}


impl KafkaProducer {
    fn safe_init_transaction(&self, producer: FutureProducer) -> Result<FutureProducer, anyhow::Error> {
        loop {
            match producer.init_transactions(Timeout::Never){
                Ok(_) => {
                    println!("kafka: initializing a transactional state with kafka {} producer", self.kind.get_route());
                    assert_reachable!("Initialized kafka producer with transactional state", &json!({"failed": format!("{}", self.kind.get_route())}));
                    break;
                }
                // Err(KafkaError::ClientConfig(e, ..) | KafkaError::MessageProduction(e)) => { // Properly handle all error variants in future instead of retrying like a dum dum https://docs.rs/rdkafka/latest/rdkafka/error/enum.KafkaError.html
                //     eprintln!("kafka: failed to initialize transactional state with kafka {} producer", self.kind.get_route());
                //     assert_unreachable!("Unrecoverable error during Kafka producer transactional state initialization", &json!({"error": format!("{} {:?}", self.kind.get_route(), e)}));
                //     return Err(anyhow!("Unrecoverable {} producer transactional state initialization error: {:?}", self.kind.get_route(), e));
                // }
                Err(e) => {
                    if is_fatal_error(&e) {
                        eprintln!("kafka: failed to initialize transactional state with kafka {} producer", self.kind.get_route());
                        assert_unreachable!("Unrecoverable error during Kafka producer transactional state initialization", &json!({"error": format!("{} {:?}", self.kind.get_route(), e)}));
                        return Err(anyhow!("Unrecoverable {} producer transactional state initialization error: {:?}", self.kind.get_route(), e));
                    } else {
                        eprintln!("kafka: failed to initialize transactional state {} producer {:?}, retrying...",self.kind.get_route(), e);
                        sleep(Duration::from_secs(1)); // Avoid tight retry loop
                    }
                }
            }
        }
        Ok(producer)
    }

    fn create_config(&mut self, brokers: &Vec<&str>) {
        let config = match self.kind {
            KafkaProducers::ExactlyOnce => {
                let producer = ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .set("transactional.id", "eactly_once_id")
                    .set("enable.idempotence", "true")
                    .set("retries", "5") // For exactly-once, higher retries
                    .set("acks", "all")  // Ensure all replicas acknowledge
                    .create::<FutureProducer>()
                    .expect("kafka: exactly once producer creation error");
                println!("kafka: creating exactly once kafka producer");
                assert_reachable!("Created \'Exactly Once\' Kafka producer", &json!({}));
                self.safe_init_transaction(producer).unwrap()
            },
            KafkaProducers::ExactlyOnceBatch => {
                let producer = ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .set("transactional.id", "eactly_once_id")
                    .set("enable.idempotence", "true")
                    .set("retries", "5") // For exactly-once, higher retries
                    .set("acks", "all")
                    .set("batch.size", "16384") // 16 KB batch size
                    .set("linger.ms", "5") // Wait for 5ms before sending a batch, useful with for loops
                    .set("compression.type", "gzip")  // Ensure all replicas acknowledge
                    .create::<FutureProducer>()
                    .expect("exactly once batch producer creation error");
                println!("kafka: creating exactly once batch kafka producer");
                assert_reachable!("Created \'Exactly Once Batch\' Kafka producer", &json!({}));
                self.safe_init_transaction(producer).unwrap()
            },
            KafkaProducers::AtLeastOnce => {
                let producer = ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .create()
                    .expect("kafka: atleast once producer creation error");
                println!("kafka: creating atleast once kafka producer");
                assert_reachable!("Created \'Atleast Once\' Kafka producer", &json!({}));
                producer
            },
            KafkaProducers::AtLeastOnceBatch => {
                let producer = ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .set("batch.size", "16384") // 16 KB batch size
                    .set("linger.ms", "5") // Wait for 5ms before sending a batch, useful with for loops
                    .set("compression.type", "gzip") // Use gzip compression
                    .create()
                    .expect("kafka: atleast once batch producer creation error");
                println!("kafka: creating atleast once batch kafka producer");
                assert_reachable!("Created \'Atleast Once Batch\' Kafka producer", &json!({}));
                producer
            }
        };
        self.producer = Some(config);
    }

    fn get_route(&self) -> &'static str {
        self.kind.get_route()
    }

    pub async fn safe_send(&self, topic: &str, msg: &str, key: &str) -> Result<(), anyhow::Error> {
        match self.kind {
            KafkaProducers::ExactlyOnce | KafkaProducers::ExactlyOnceBatch => {
                loop {
                    match self.producer.as_ref().unwrap().begin_transaction() {
                        Ok(_) => {
                            println!("kafka: successfully began transaction with topic {} id {} msg {}", &topic, &key, &msg);
                            assert_sometimes!(true, "Successfully began transaction", &json!({"result": format!("topic {}, id {}, msg {}", &topic, &key, &msg)}));
                            break;
                        },
                        Err(e) => {
                            eprintln!("kafka: failed to begin transaction with topic {} id {} msg {}: retrying..., error: {}", &topic, &key, &msg, e);
                            assert_sometimes!(false, "Successfully began transaction", &json!({"error": format!("{}", e)}));
                            sleep(Duration::from_secs(1));
                        }
                    }
                }
                loop {
                    let send_status = self.producer.as_ref().unwrap().send(
                        FutureRecord::to(topic)
                            .payload(msg)
                            .key(key)
                            .partition(0),
                        Duration::from_secs(60),
                    ).await;
                    match send_status {
                        Ok(_) => {
                            println!("kafka: successfully produced transaction with topic {}, id {}, msg {}", &topic, &key, &msg);
                            assert_sometimes!(true, "Successfully produced transaction", &json!({"result": format!("topic {}, id {}, msg {}", &topic, &key, &msg)}));
                            break;
                        },
                        Err((e, _message)) => {
                            eprintln!("kafka: failed to produce transaction with topic {}, id {}, msg {}, error {}, retrying..., ", &topic, &key, &msg, e);
                            assert_sometimes!(false, "Successfully produced transaction", &json!({"error": format!("{}", e)}));
                            sleep(Duration::from_secs(1));
                        }
                    }
                }
                loop {
                    match self.producer.as_ref().unwrap().commit_transaction(Timeout::Never) {
                        Ok(_) => {
                            println!("kafka: transaction committed successfully with topic {}, id {}, msg {}", &topic, &key, &msg);
                            assert_sometimes!(true, "Successfully committed transaction", &json!({"result": format!("topic {}, id {}, msg {}", &topic, &key, &msg)}));
                            break;
                        }
                        // Err(KafkaError::Transaction(TransactionError::ProducerFenced) | KafkaError::Transaction(TransactionError::TransactionAborted)) => {
                        //     eprintln!("Unrecoverable error during commit: {:?}", e);
                            // // Best effort abort, then exit or propagate error
                            // let _ = self.producer.unwrap().abort_transaction(Timeout::Never);
                            // assert_unreachable!("Unrecoverable error during Kafka commit", &json!({"error": format!("{:?}", e)}));
                            // return Err(anyhow!("Unrecoverable transaction error: {:?}", e));
                        // }
                        Err(e) => {
                            eprintln!("kafka: failed to commit transaction with topic {}, id {}, msg {}, error {}, retrying...", &topic, &key, &msg, e);
                            if is_fatal_error(&e) {
                                eprintln!("Unrecoverable error during commit: {:?}", e);
                                // Best effort abort, then exit or propagate error
                                let _ = self.producer.as_ref().unwrap().abort_transaction(Timeout::Never);
                                assert_unreachable!("Unrecoverable error during Kafka commit", &json!({"error": format!("{:?}", e)}));
                                return Err(anyhow!("Unrecoverable transaction error: {:?}", e));
                            }
                            assert_sometimes!(false, "Successfully committed transaction", &json!({"error": format!("{}", e)}));
                            sleep(Duration::from_secs(1)); // Avoid tight retry loop
                        }
                    }
                }
                Ok(())
            }
            KafkaProducers::AtLeastOnce | KafkaProducers::AtLeastOnceBatch => {
                loop {
                    let send_status = self.producer.as_ref().unwrap().send(
                        FutureRecord::to(topic)
                            .payload(msg)
                            .key(key),
                        Duration::from_secs(60),
                    ).await;
                    match send_status {
                        Ok(_) => {
                            println!("kafka: successfully produced message with topic {}, id {}, msg {}", &topic, &key, &msg);
                            assert_sometimes!(true, "Successfully produced message", &json!({"result": format!("topic {}, id {}, msg {}", &topic, &key, &msg)}));
                            break;
                        },
                        Err((e, _message)) => {
                            eprintln!("kafka: failed to produce message with topic {}, id {}, msg {}, error {}, retrying...", &topic, &key, &msg, e);
                            assert_sometimes!(false, "Successfully produced message", &json!({"error": format!("{}", e)}));
                            sleep(Duration::from_secs(1)); // Avoid tight retry loop
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct BankData {
    id: i32,
    data: String, 
    produced_timestamp: Option<SystemTime>,
    producer_type: Option<String>,
    consumed_timestamp: Option<SystemTime>,
    consumer_type: Option<String>,
    consumed_count: i32,
    topic: String
}



impl BankData {
    fn from_row(row: &Row) -> Self { 
        BankData {
            id: row.get("id"),
            data: row.get("data"), 
            produced_timestamp: row.try_get("produced_timestamp").ok(),
            producer_type: row.try_get("producer_type").ok(),
            consumed_timestamp: row.try_get("consumed_timestamp").ok(),
            consumer_type: row.try_get("consumer_type").ok(),
            consumed_count: row.get("consumed_count"),
            topic: row.get("topic")
        }
    }
}



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

    async fn safe_get_unproduced_data(&self, limit: i64) -> Vec<BankData> {
        let query = "
            SELECT * 
            FROM producer 
            WHERE produced_timestamp IS NULL 
            ORDER BY id ASC 
            LIMIT $1;
        ";
        let rows = loop {
            let result = self.client
                .query(query, &[&(limit)])
                .await;
            match result {
                Ok(rows) => {
                    let rows_ids: Vec<i32> = rows.iter().map(|row| row.get("id")).collect::<Vec<_>>();
                    println!("postgres: unproduced data query completed successfully: {:?}", rows_ids);
                    assert_sometimes!(true, "Producer recieved data", &json!({"result": rows_ids}));
                    break rows;
                },
                Err(e) => {
                    eprintln!("postgres: failed to perform query, err: {}, retrying...", e);
                    assert_sometimes!(false, "Producer recieved data", &json!({"error": format!("Failed to perform query: {}", e)}));
                    sleep(Duration::from_secs(1)); // Avoid tight retry loop
                }
            }
        };
        let accounts: Vec<BankData> = rows
            .iter()
            .map(BankData::from_row)
            .collect(); // Collect directly since there are no errors in `from_row`
        accounts
            // TRY USE THIS: use serde_postgres::de::from_row;
    }

    async fn safe_write_produced(&self, b_a: &BankData, route: &'static str) {
        let update = "
            UPDATE producer
            SET 
                produced_timestamp = $1,
                producer_type = $2
            WHERE id = $3;
        ";
        loop {
            let current_timestamp = chrono::Utc::now().naive_utc();
            // this performs update and returns number of rows updated. Not super useful for us, so we don't capture this number
            let pg_status = self.client
                .execute(update, &[&current_timestamp, &route, &(b_a.id)]) // Assuming id is SERIAL (i32 in DB)
                .await;
            match pg_status {
                Ok(d) => {
                    println!("postgres: produced data written successfully: id: {}, number of rows update: {}", b_a.id, d);
                    assert_sometimes!(true, "Producer recorded data produced to Kafka on state-tracker", &json!({"result": format!("id: {} number of rows updated: {}", b_a.id, d)}));
                    break;
                },
                Err(e) => {
                    eprintln!("postgres: failed to write produce data timestamp: id: {}, err: {}", b_a.id, e);
                    assert_sometimes!(false, "Producer recorded data produced to Kafka on state-tracker", &json!({"error": format!("Failed to update timestamp for id: {}, err: {}", b_a.id, e)}));
                    sleep(Duration::from_secs(1)); // Avoid tight retry loop
                }
            }
        }
    }
}


fn create_router(state: Arc<AppState>) -> Router {
    let mut r = Router::new();
    for kafka_mode in KafkaProducers::iter() {
        r = r.nest(
            kafka_mode.get_route(),
            create_nested_router(state.clone(), kafka_mode)
        )
    }
    r
}

fn create_nested_router(state: Arc<AppState>, kafka_mode: KafkaProducers) -> Router{
     Router::new()
        .route("/:topic/:num_records", post(handle))
        // .route("/single/:topic", post(handle_single))
        // .route("/batch_sequential/:topic/:", post(handle_sequential_batch))
        .with_state(state) // Attach state to the router
        .layer(middleware::from_fn(move |req, next| {
            add_context_middleware(req, next, kafka_mode.clone())
        }))
       
}

async fn add_context_middleware(
    mut req: axum::http::Request<Body>,
    next: Next,
    kafka_mode: KafkaProducers, // Pass context explicitly
) -> Response {
    info!("middleware: adding context to request");
    req.extensions_mut().insert(kafka_mode);
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
    Extension(kafka_mode): Extension<KafkaProducers>
) {
    let brokers = &state.brokers;
    
    let mut producers = state.producers.lock().await;
    let producer = producers
        .entry(kafka_mode.get_route().to_string())
        .or_insert_with(|| {
            let mut kp = KafkaProducer {kind: kafka_mode.clone(), producer: None};
            kp.create_config(brokers);
            kp
        });

    let mut pg_client_guard = state.pg_client.lock().await;
    if pg_client_guard.is_none() {
        *pg_client_guard = Some(
            Postgres::new("host=state-tracker user=u password=p dbname=d ")
            .await
            .map_err(|e| {
                eprintln!("postgres: failed to initialize Postgres {:?}", e);
                e
            })
            .unwrap()
        );
        assert_reachable!("Producer created connection to state-tracker", &json!({}));
    }

    if let Some(pg_client) = &*pg_client_guard {
        let bank_accounts = pg_client
            .safe_get_unproduced_data(num_records)
            .await;

        for bank_account in bank_accounts.iter() {
            let bank_account_str = serde_json::to_string(&bank_account)
            // Use `anyhow` ?
                .map_err(|e| {
                    eprintln!("kafka: failed to serialize BankData to string: {:?}", e);
                    assert_unreachable!("Producer failed to serialize BankData to string", &json!({"error": format!("{:?}", e)}));
                    e
                }).unwrap();
            let msg: String = format!("{}", &bank_account_str);
            let key: String = format!("{}", &bank_account.id);
            println!("kafka: data being produced");

            let d = producer.safe_send(&topic, &msg, &key).await;
            if let Ok(_) = d {
                pg_client.safe_write_produced(bank_account, kafka_mode.get_route()).await;
            }
        }

        // info!("num_bank_accounts_produced {:?}", bank_accounts.len());
        // Use pg_client.client for database operations
    }
    // The producer will automatically batch before sending messages
    // Kafka itself handles batching efficiently at the partition level. 
    // By increasing the producer's throughput configurations, you can leverage Kafka's internal batching mechanisms
}

// API endpoint to get selected mode's history etc. this endpoint will be called by the consumer to make sure everything has been consumed

#[tokio::main]
async fn main() {
    antithesis_init();
    println!("{:?}", SystemTime::now().duration_since(UNIX_EPOCH));
    env_logger::init();

    let producers: Arc<Mutex<HashMap<String, KafkaProducer>>> = Arc::new(Mutex::new(HashMap::new()));
    let brokers = vec!["kafka-3:9092", "kafka-2:9092", "kafka-1:9092"];
    let pg_client = Arc::new(Mutex::new(None));

    let state = AppState { producers, brokers, pg_client };
    let app = create_router(state.into());

    // let app = 

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server running at http:{:?}", listener);
    
    axum::serve(listener, app).await.unwrap();    
}
