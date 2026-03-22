use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};

use std::fmt::Write;

use clap::{App, Arg};
use log::{info, error, debug, warn};

use rdkafka::config::ClientConfig;
use rdkafka::message::{OwnedHeaders, Header, Headers};
use rdkafka::{ClientContext};
use rdkafka::consumer::{Consumer, StreamConsumer, Rebalance, BaseConsumer, ConsumerContext, CommitMode};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::producer::Producer;
use rdkafka::util::Timeout;
use rdkafka::error::KafkaError;
use rdkafka::TopicPartitionList;
use rdkafka::error::KafkaResult;
use rdkafka::Message;

use env_logger;

use reqwest;

use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;

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
use tokio_postgres::{NoTls, Row};

use std::thread::sleep;
use chrono::NaiveDateTime;

use postgres_types::{Type, IsNull};
use anyhow::{anyhow, Result, Context};

use chrono;


// ============================================================
// Kafka Error Handling (same pattern as producer)
// ============================================================

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
        e => {
            println!("kafka: non fatal error {}", e);
            false
        }
    }
}


// ============================================================
// Custom Consumer Context (same pattern as consumer)
// ============================================================

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

type LoggingConsumer = StreamConsumer<CustomContext>;


// ============================================================
// Data Types
// ============================================================

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
struct EnrichedBankData {
    // Original fields
    id: i32,
    data: String,
    produced_timestamp: Option<SystemTime>,
    producer_type: Option<String>,
    consumed_timestamp: Option<SystemTime>,
    consumer_type: Option<String>,
    consumed_count: i32,
    topic: String,

    // Enrichment fields
    risk_score: f64,
    transaction_category: String,
    processed_timestamp: String,
    processor_type: String,
}


// ============================================================
// Enrichment Logic
// ============================================================

fn enrich(b_d: &BankData, processor_type: &str) -> EnrichedBankData {
    let processed_timestamp = chrono::Utc::now().naive_utc().to_string();

    let (transaction_category, risk_score) = match serde_json::from_str::<Value>(&b_d.data) {
        Ok(val) => {
            if val.get("iban").is_some() {
                // BankFund transaction
                let amount = val.get("amount").and_then(|a| a.as_f64()).unwrap_or(0.0);
                let category = "fund".to_string();
                let risk = if amount > 5000.0 { 0.8 } else if amount > 1000.0 { 0.5 } else { 0.1 };
                (category, risk)
            } else if val.get("from").is_some() && val.get("to").is_some() {
                // BankTransfer transaction
                let amount = val.get("amount").and_then(|a| a.as_f64()).unwrap_or(0.0);
                let from = val.get("from").and_then(|f| f.as_str()).unwrap_or("");
                let to = val.get("to").and_then(|t| t.as_str()).unwrap_or("");

                let category = if from == to {
                    "transfer_self".to_string()
                } else if amount > 50.0 {
                    "transfer_large".to_string()
                } else {
                    "transfer_small".to_string()
                };

                let risk = match category.as_str() {
                    "transfer_self" => 0.9,
                    "transfer_large" => 0.6,
                    "transfer_small" => 0.2,
                    _ => 0.5,
                };
                (category, risk)
            } else {
                ("unknown".to_string(), 0.5)
            }
        }
        Err(_) => ("parse_error".to_string(), 1.0),
    };

    EnrichedBankData {
        id: b_d.id,
        data: b_d.data.clone(),
        produced_timestamp: b_d.produced_timestamp,
        producer_type: b_d.producer_type.clone(),
        consumed_timestamp: b_d.consumed_timestamp,
        consumer_type: b_d.consumer_type.clone(),
        consumed_count: b_d.consumed_count,
        topic: b_d.topic.clone(),
        risk_score,
        transaction_category,
        processed_timestamp,
        processor_type: processor_type.to_string(),
    }
}


// ============================================================
// Processor Modes
// ============================================================

#[derive(Clone, Debug, EnumIter)]
enum ProcessorModes {
    ExactlyOnceProcessor,
    AtLeastOnceProcessor,
}

impl ProcessorModes {
    fn get_route(&self) -> &'static str {
        match self {
            ProcessorModes::ExactlyOnceProcessor => "/exactly_once_processor",
            ProcessorModes::AtLeastOnceProcessor => "/atleast_once_processor",
        }
    }
}


// ============================================================
// Kafka Consumer (consuming from raw topics)
// ============================================================

struct ProcessorConsumer {
    kind: ProcessorModes,
    consumer: Option<LoggingConsumer>
}

impl ProcessorConsumer {
    fn create_config(&mut self, brokers: &Vec<&str>) {
        let config = match self.kind {
            ProcessorModes::AtLeastOnceProcessor => {
                let c = ClientConfig::new()
                    .set("group.id", "4")
                    .set("bootstrap.servers", brokers.join(","))
                    .set("enable.partition.eof", "false")
                    .set("session.timeout.ms", "30000")
                    .set("enable.auto.commit", "true")
                    .set("auto.offset.reset", "earliest")
                    .create_with_context(CustomContext)
                    .expect("Processor consumer creation failed");
                println!("kafka: creating atleast once processor consumer");
                assert_reachable!("Created \'Atleast Once\' processor Kafka consumer", &json!({}));
                c
            },
            ProcessorModes::ExactlyOnceProcessor => {
                let c = ClientConfig::new()
                    .set("group.id", "3")
                    .set("bootstrap.servers", brokers.join(","))
                    .set("enable.partition.eof", "false")
                    .set("session.timeout.ms", "30000")
                    .set("isolation.level", "read_committed")
                    .set("enable.auto.commit", "false")
                    .set("auto.offset.reset", "earliest")
                    .create_with_context(CustomContext)
                    .expect("Processor consumer creation failed");
                println!("kafka: creating exactly once processor consumer");
                assert_reachable!("Created \'Exactly Once\' processor Kafka consumer", &json!({}));
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
                    println!("kafka: processor subscribed to topic {}", &topic);
                    assert_sometimes!(true, "Processor subscribed to topic", &json!({"result": format!("none")}));
                    break
                },
                Err(e) => {
                    eprintln!("kafka: processor can't subscribe to topic: {}", e);
                    assert_sometimes!(false, "Processor subscribed to topic", &json!({"error": format!("Can't subscribe to specified topics: {}", e)}));
                    sleep(Duration::from_secs(1));
                }
            }
        }
    }

    async fn safe_get_consumed(&mut self) -> Result<(BankData, NaiveDateTime)> {
        let message = self.consumer.as_ref().unwrap().recv().await
            .map_err(|e| {
                assert_sometimes!(false, "Processor consumed data from raw topic", &json!({ "error": format!("{:?}", e) }));
                e
            })?;

        assert_sometimes!(true, "Processor consumed data from raw topic", &json!({ "value": format!("{:?}", message) }));

        let payload_str = match message.payload_view::<str>() {
            Some(Ok(s)) => {
                println!("kafka: processor consumed message has correct string decoding");
                assert_sometimes!(true, "Processor consumed message has correct string decoding", &json!({ "value": s }));
                s
            },
            Some(Err(e)) => {
                eprintln!("kafka: processor consumed message has incorrect string decoding");
                // assert_sometimes!(false, "Processor consumed message failed to decode payload", &json!({ "error": format!("{:?}", e) }));
                return Err(e.into());
            },
            None => {
                eprintln!("kafka: processor consumed message has no payload");
                // assert_sometimes!(false, "Processor consumed message payload is None", &json!({ "error": "none" }));
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Kafka payload is None").into());
            }
        };

        match self.kind {
            ProcessorModes::ExactlyOnceProcessor => {
                self.consumer.as_ref().unwrap().commit_message(&message, CommitMode::Sync)
                    .map_err(|e| {
                        println!("kafka: processor failed to commit message offset");
                        // assert_sometimes!(false, "Processor failed to commit offset", &json!({ "error": format!("{:?}", e) }));
                        e
                    })?;
            },
            _ => {
                println!("kafka: processor not committing, letting auto commit take care of it");
            }
        }

        let current_timestamp = chrono::Utc::now().naive_utc();

        let b_d: BankData = serde_json::from_str(payload_str.trim())
            .map_err(|e| {
                println!("kafka: processor consumed message failed to deserialize: {:?}", e);
                // assert_sometimes!(false, "Processor consumed message failed to deserialize to BankData", &json!({ "error": format!("{:?}", e) }));
                anyhow::Error::from(e)
            })?;

        println!("kafka: processor consumed message deserializable to BankData: {:?}", b_d);
        assert_sometimes!(true, "Processor consumed message deserialized to BankData", &json!({ "result": format!("{:?}", b_d) }));

        Ok((b_d, current_timestamp))
    }
}


// ============================================================
// Kafka Producer (producing to enriched topics)
// ============================================================

struct ProcessorProducer {
    kind: ProcessorModes,
    producer: Option<FutureProducer>
}

impl ProcessorProducer {
    fn safe_init_transaction(&self, producer: FutureProducer) -> Result<FutureProducer, anyhow::Error> {
        loop {
            match producer.init_transactions(Timeout::Never) {
                Ok(_) => {
                    println!("kafka: initializing a transactional state with processor {} producer", self.kind.get_route());
                    assert_reachable!("Initialized processor Kafka producer with transactional state", &json!({"route": format!("{}", self.kind.get_route())}));
                    break;
                }
                Err(e) => {
                    if is_fatal_error(&e) {
                        eprintln!("kafka: failed to initialize transactional state with processor {} producer", self.kind.get_route());
                        assert_unreachable!("Unrecoverable error during processor Kafka producer transactional state initialization", &json!({"error": format!("{} {:?}", self.kind.get_route(), e)}));
                        return Err(anyhow!("Unrecoverable {} producer transactional state initialization error: {:?}", self.kind.get_route(), e));
                    } else {
                        eprintln!("kafka: failed to initialize transactional state {} producer {:?}, retrying...", self.kind.get_route(), e);
                        sleep(Duration::from_secs(1));
                    }
                }
            }
        }
        Ok(producer)
    }

    fn create_config(&mut self, brokers: &Vec<&str>) {
        let config = match self.kind {
            ProcessorModes::ExactlyOnceProcessor => {
                let producer = ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .set("transactional.id", "processor_eo_id")
                    .set("enable.idempotence", "true")
                    .set("retries", "5")
                    .set("acks", "all")
                    .create::<FutureProducer>()
                    .expect("kafka: processor exactly once producer creation error");
                println!("kafka: creating exactly once processor producer");
                assert_reachable!("Created \'Exactly Once\' processor Kafka producer", &json!({}));
                self.safe_init_transaction(producer).unwrap()
            },
            ProcessorModes::AtLeastOnceProcessor => {
                let producer = ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .create()
                    .expect("kafka: processor atleast once producer creation error");
                println!("kafka: creating atleast once processor producer");
                assert_reachable!("Created \'Atleast Once\' processor Kafka producer", &json!({}));
                producer
            }
        };
        self.producer = Some(config);
    }

    pub async fn safe_send(&self, topic: &str, msg: &str, key: &str) -> Result<(), anyhow::Error> {
        match self.kind {
            ProcessorModes::ExactlyOnceProcessor => {
                loop {
                    match self.producer.as_ref().unwrap().begin_transaction() {
                        Ok(_) => {
                            println!("kafka: processor successfully began transaction with topic {} id {} msg {}", &topic, &key, &msg);
                            assert_sometimes!(true, "Processor successfully began transaction", &json!({"result": format!("topic {}, id {}", &topic, &key)}));
                            break;
                        },
                        Err(e) => {
                            eprintln!("kafka: processor failed to begin transaction: retrying..., error: {}", e);
                            assert_sometimes!(false, "Processor successfully began transaction", &json!({"error": format!("{}", e)}));
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
                            println!("kafka: processor successfully produced enriched message to topic {}, id {}", &topic, &key);
                            assert_sometimes!(true, "Processor produced enriched message to output topic", &json!({"result": format!("topic {}, id {}", &topic, &key)}));
                            break;
                        },
                        Err((e, _message)) => {
                            eprintln!("kafka: processor failed to produce enriched message, error {}, retrying...", e);
                            assert_sometimes!(false, "Processor produced enriched message to output topic", &json!({"error": format!("{}", e)}));
                            sleep(Duration::from_secs(1));
                        }
                    }
                }
                loop {
                    match self.producer.as_ref().unwrap().commit_transaction(Timeout::Never) {
                        Ok(_) => {
                            println!("kafka: processor transaction committed successfully with topic {}, id {}", &topic, &key);
                            assert_sometimes!(true, "Processor successfully committed transaction", &json!({"result": format!("topic {}, id {}", &topic, &key)}));
                            break;
                        }
                        Err(e) => {
                            eprintln!("kafka: processor failed to commit transaction, error {}, retrying...", e);
                            if is_fatal_error(&e) {
                                eprintln!("Unrecoverable error during processor commit: {:?}", e);
                                let _ = self.producer.as_ref().unwrap().abort_transaction(Timeout::Never);
                                assert_unreachable!("Unrecoverable error during processor Kafka commit", &json!({"error": format!("{:?}", e)}));
                                return Err(anyhow!("Unrecoverable processor transaction error: {:?}", e));
                            }
                            assert_sometimes!(false, "Processor successfully committed transaction", &json!({"error": format!("{}", e)}));
                            sleep(Duration::from_secs(1));
                        }
                    }
                }
                Ok(())
            }
            ProcessorModes::AtLeastOnceProcessor => {
                loop {
                    let send_status = self.producer.as_ref().unwrap().send(
                        FutureRecord::to(topic)
                            .payload(msg)
                            .key(key),
                        Duration::from_secs(60),
                    ).await;
                    match send_status {
                        Ok(_) => {
                            println!("kafka: processor successfully produced enriched message to topic {}, id {}", &topic, &key);
                            assert_sometimes!(true, "Processor produced enriched message to output topic", &json!({"result": format!("topic {}, id {}", &topic, &key)}));
                            break;
                        },
                        Err((e, _message)) => {
                            eprintln!("kafka: processor failed to produce enriched message, error {}, retrying...", e);
                            assert_sometimes!(false, "Processor produced enriched message to output topic", &json!({"error": format!("{}", e)}));
                            sleep(Duration::from_secs(1));
                        }
                    }
                }
                Ok(())
            }
        }
    }
}


// ============================================================
// PostgreSQL (processor's own table)
// ============================================================

#[derive(Debug)]
struct Postgres {
    client: tokio_postgres::Client
}

impl Postgres {
    pub async fn new(connection_string: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
        tokio::spawn(connection);
        Ok(Postgres { client })
    }

    async fn safe_create_processor_table(&self) {
        let query = "
            CREATE TABLE IF NOT EXISTS processor (
                id INT PRIMARY KEY,
                risk_score DOUBLE PRECISION NOT NULL,
                transaction_category VARCHAR(255) NOT NULL,
                processed_timestamp TIMESTAMP NOT NULL,
                processor_type VARCHAR(255) NOT NULL,
                output_topic VARCHAR(255) NOT NULL
            );
        ";
        loop {
            match self.client.execute(query, &[]).await {
                Ok(_) => {
                    println!("postgres: processor table created successfully");
                    assert_reachable!("Processor created processor table on state-tracker", &json!({}));
                    break;
                }
                Err(e) => {
                    eprintln!("postgres: failed to create processor table: {}, retrying...", e);
                    sleep(Duration::from_secs(1));
                }
            }
        }
    }

    async fn safe_write_processed(&self, enriched: &EnrichedBankData, output_topic: &str) {
        let upsert = "
            INSERT INTO processor (id, risk_score, transaction_category, processed_timestamp, processor_type, output_topic)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
                risk_score = EXCLUDED.risk_score,
                transaction_category = EXCLUDED.transaction_category,
                processed_timestamp = EXCLUDED.processed_timestamp,
                processor_type = EXCLUDED.processor_type,
                output_topic = EXCLUDED.output_topic;
        ";
        loop {
            let current_timestamp = chrono::Utc::now().naive_utc();
            let result = self.client
                .execute(upsert, &[
                    &enriched.id,
                    &enriched.risk_score,
                    &enriched.transaction_category,
                    &current_timestamp,
                    &enriched.processor_type,
                    &output_topic,
                ])
                .await;
            match result {
                Ok(d) => {
                    println!("postgres: processor recorded enriched data: id: {}, rows affected: {}", enriched.id, d);
                    assert_sometimes!(true, "Processor recorded processing state to state-tracker", &json!({"result": format!("id: {} rows: {}", enriched.id, d)}));
                    break;
                },
                Err(e) => {
                    eprintln!("postgres: failed to write processed data: id: {}, err: {}", enriched.id, e);
                    assert_sometimes!(false, "Processor recorded processing state to state-tracker", &json!({"error": format!("Failed to upsert for id: {}, err: {}", enriched.id, e)}));
                    sleep(Duration::from_secs(1));
                }
            }
        }
    }
}


// ============================================================
// AppState
// ============================================================

#[derive(Clone)]
struct AppState {
    consumers: Arc<Mutex<HashMap<String, ProcessorConsumer>>>,
    producers: Arc<Mutex<HashMap<String, ProcessorProducer>>>,
    brokers: Vec<&'static str>,
    pg_client: Arc<Mutex<Option<Postgres>>>,
}


// ============================================================
// Router (same pattern as producer/consumer)
// ============================================================

fn create_router(state: Arc<AppState>) -> Router {
    let mut r = Router::new();
    for mode in ProcessorModes::iter() {
        r = r.nest(
            mode.get_route(),
            create_nested_router(state.clone(), mode)
        )
    }
    r
}

fn create_nested_router(state: Arc<AppState>, mode: ProcessorModes) -> Router {
    Router::new()
        .route("/:topic/:num_records", post(handle))
        .with_state(state)
        .layer(middleware::from_fn(move |req, next| {
            add_context_middleware(req, next, mode.clone())
        }))
}

async fn add_context_middleware(
    mut req: axum::http::Request<Body>,
    next: Next,
    context: ProcessorModes,
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


// ============================================================
// Handle: consume -> enrich -> produce -> record
// ============================================================

async fn handle(
    Path((topic, num_records)): Path<(String, i64)>,
    State(state): State<Arc<AppState>>,
    Extension(processor_mode): Extension<ProcessorModes>
) {
    let brokers = &state.brokers;
    let output_topic = format!("{}_enriched", &topic);

    // Get or create consumer
    println!("kafka: processor waiting for consumer lock");
    let mut consumers = state.consumers.lock().await;
    let consumer = consumers
        .entry(processor_mode.get_route().to_string())
        .or_insert_with(|| {
            let mut pc = ProcessorConsumer { kind: processor_mode.clone(), consumer: None };
            pc.create_config(brokers);
            pc
        });

    // Get or create producer
    let mut producers = state.producers.lock().await;
    let producer = producers
        .entry(processor_mode.get_route().to_string())
        .or_insert_with(|| {
            let mut pp = ProcessorProducer { kind: processor_mode.clone(), producer: None };
            pp.create_config(brokers);
            pp
        });

    // Get or create postgres client
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
        assert_reachable!("Processor created connection to state-tracker", &json!({}));
        pg_client_guard.as_ref().unwrap().safe_create_processor_table().await;
    }

    if let Some(pg_client) = &*pg_client_guard {
        consumer.safe_subscribe(&topic);
        let mut count = 0;
        while count < num_records {
            let b_d_data = consumer.safe_get_consumed().await;
            match b_d_data {
                Ok((b_d, _timestamp)) => {
                    // Enrich the data
                    let enriched = enrich(&b_d, processor_mode.get_route());
                    assert_sometimes!(true, "Processor enriched message successfully", &json!({"id": enriched.id, "category": &enriched.transaction_category, "risk_score": enriched.risk_score}));

                    // Serialize and produce to enriched topic
                    let enriched_str = serde_json::to_string(&enriched)
                        .map_err(|e| {
                            eprintln!("kafka: processor failed to serialize EnrichedBankData to string: {:?}", e);
                            assert_unreachable!("Processor failed to serialize EnrichedBankData to string", &json!({"error": format!("{:?}", e)}));
                            e
                        }).unwrap();
                    let key = format!("{}", &enriched.id);

                    let d = producer.safe_send(&output_topic, &enriched_str, &key).await;
                    if let Ok(_) = d {
                        pg_client.safe_write_processed(&enriched, &output_topic).await;
                    }
                }
                Err(e) => {
                    eprintln!("kafka: processor could not consume data error: {:?}, retrying ...", e);
                    sleep(Duration::from_secs(1));
                }
            }
            count += 1;
        }
    }
}


// ============================================================
// Main
// ============================================================

#[tokio::main]
async fn main() {
    antithesis_init();
    println!("{:?}", SystemTime::now().duration_since(UNIX_EPOCH));
    env_logger::init();

    let consumers: Arc<Mutex<HashMap<String, ProcessorConsumer>>> = Arc::new(Mutex::new(HashMap::new()));
    let producers: Arc<Mutex<HashMap<String, ProcessorProducer>>> = Arc::new(Mutex::new(HashMap::new()));
    let brokers = vec!["kafka-3:9092", "kafka-2:9092", "kafka-1:9092"];
    let pg_client = Arc::new(Mutex::new(None));

    let state = AppState { consumers, producers, brokers, pg_client };
    let app = create_router(state.into());

    println!("Starting Processor");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server running at http:{:?}", listener);

    axum::serve(listener, app).await.unwrap();
}
