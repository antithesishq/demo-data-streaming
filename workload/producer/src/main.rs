use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};

use std::fmt::Write;

use clap::{App, Arg};
use log::{debug, error, info, warn};

use rdkafka::config::ClientConfig;
use rdkafka::message::{OwnedHeaders, Header, Headers};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use env_logger;

use reqwest;

use tokio::runtime::Handle;
use tokio::signal::unix::SignalKind;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
// use std::error::Error;

use std::net::SocketAddr;
use rdkafka::producer::BaseProducer;

use axum::{
    body::Body,
    extract::{State, Json, Extension, Path},
    middleware::{self, Next},
    response::{Response, Json as ResponseJson},
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
    producers: Arc<Mutex<HashMap<String, FutureProducer>>>,
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
    fn create_config(&self, brokers: &Vec<&str>) -> FutureProducer {
        match self {
            KafkaProducers::ExactlyOnce => {
                let c = ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .set("debug", "all")
                    .set("retries", "5") // For exactly-once, higher retries
                    .set("acks", "all")  // Ensure all replicas acknowledge
                    .create::<FutureProducer>()
                    .expect("exactly once producer creation error");
                assert_reachable!("Created \'Exactly Once\' Kafka producer", &json!({}));
                c
            },
            KafkaProducers::ExactlyOnceBatch => {
                let c = ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .set("debug", "all")
                    .set("retries", "5") // For exactly-once, higher retries
                    .set("acks", "all")
                    .set("batch.size", "16384") // 16 KB batch size
                    .set("linger.ms", "5") // Wait for 5ms before sending a batch, useful with for loops
                    .set("compression.type", "gzip")  // Ensure all replicas acknowledge
                    .create::<FutureProducer>()
                    .expect("exactly once batch producer creation error");
                assert_reachable!("Created \'Exactly Once Batch\' Kafka producer", &json!({}));
                c
            },
            KafkaProducers::AtLeastOnce => {
                let c = ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .set("debug", "all")
                    .create()
                    .expect("exactly once producer creation error");
                assert_reachable!("Created \'Atleast Once\' Kafka producer", &json!({}));
                c
            },
            KafkaProducers::AtLeastOnceBatch => {
                let c = ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .set("debug", "all")
                    .set("batch.size", "16384") // 16 KB batch size
                    .set("linger.ms", "5") // Wait for 5ms before sending a batch, useful with for loops
                    .set("compression.type", "gzip") // Use gzip compression
                    .create()
                    .expect("atleast once batch producer creation error");
                assert_reachable!("Created \'Atleast Once Batch\' Kafka producer", &json!({}));
                c
            }
        }
    }

    fn get_route(&self) -> &'static str {
        match self {
            KafkaProducers::ExactlyOnce => "/exactly_once_single",
            KafkaProducers::ExactlyOnceBatch => "/exactly_once_batch",
            KafkaProducers::AtLeastOnce => "/atleast_once_single",
            KafkaProducers::AtLeastOnceBatch => "/atleast_once_batch"
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

    async fn get_unproduced_data(&self, limit: i64) -> Result<Vec<BankAccount>, Error> {
        let query = "
            SELECT * 
            FROM producer 
            WHERE produced_timestamp IS NULL 
            ORDER BY id ASC 
            LIMIT $1;
        ";
        let rows = self.client
        .query(query, &[&(limit)])
        .await
        .map_err(|e| {
            error!("Failed to perform query: {}", e);
            assert_sometimes!(false, "Producer recieved data from state_tracker", &json!({"error": format!("Failed to perform query: {}", e)}));
            e
        })?;
        let accounts: Vec<BankAccount> = rows
            .iter()
            .map(BankAccount::from_row)
            .collect(); // Collect directly since there are no errors in `from_row`
        assert_sometimes!(true, "Producer recieved data from state_tracker", &json!({"result": accounts.len()}));
        Ok(accounts) // Wrap in `Ok` because this function returns a Result
            // TRY USE THIS: use serde_postgres::de::from_row;
    }

    async fn write_produced(&self, b_a: &BankAccount, mode: &KafkaProducers) -> Result<(), Error> {
        let update = "
            UPDATE producer
            SET 
                produced_timestamp = $1,
                producer_type = $2
            WHERE id = $3;
        ";
        let current_timestamp = chrono::Utc::now().naive_utc();
        // this performs update and returns number of rows updated. Not super useful for us, so we don't capture this number
        self.client
            .execute(update, &[&current_timestamp, &mode.get_route(), &(b_a.id)]) // Assuming id is SERIAL (i32 in DB)
            .await
            .and_then(|d| {
                assert_sometimes!(true, "Producer recorded data produced to Kafka on state_tracker", &json!({"result": b_a}));
                Ok(d)
            })
            .map_err(|e| {
                error!("Failed to update timestamp: {}", e);
                assert_sometimes!(false, "Producer recorded data produced to Kafka on state_tracker", &json!({"error": format!("Failed to update timestamp for {:?} {}", b_a, e)}));
                e
            })?;
        Ok(())
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

fn create_nested_router(state: Arc<AppState>, mode: KafkaProducers) -> Router{
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
    context: KafkaProducers, // Pass context explicitly
) -> Response {
    info!("middleware: adding context to request");
    req.extensions_mut().insert(context);
    next.run(req).await
}

async fn handle(
    Path((topic, num_records)): Path<(String, i64)>,
    State(state): State<Arc<AppState>>,
    Extension(mode): Extension<KafkaProducers>
) {
    let brokers = &state.brokers;
    
    let mut producers = state.producers.lock().await;
    let producer = producers
        .entry(mode.get_route().to_string())
        .or_insert_with(|| mode.create_config(brokers));

    let mut pg_client_guard = state.pg_client.lock().await;
    if pg_client_guard.is_none() {
        *pg_client_guard = Some(
            Postgres::new("host=state_tracker user=u password=p dbname=d ")
            .await
            .map_err(|e| {
                error!("Failed to initialize Postgres {:?}", e);
                e
            })
            .unwrap()
        );
        assert_reachable!("Producer created connection to state_tracker", &json!({}));
    }

    if let Some(pg_client) = &*pg_client_guard {
        let bank_accounts = pg_client
            .get_unproduced_data(num_records)
            .await
            .unwrap();

        for bank_account in bank_accounts.iter() {
            let bank_account_str = serde_json::to_string(&bank_account)
            // Use `anyhow` ?
                .map_err(|e| {
                    error!("Failed to serialize BankAccount to JSON {:?}", e);
                    assert_unreachable!("Producer failed to serialize BankAccount to JSON", &json!({"error": format!("Failed to serialize BankAccount to JSON {:?}", e)}));
                    e
                }).unwrap();
            let msg: String = format!("{}", &bank_account_str);
            let key: String = format!("{}", &bank_account.id);
            info!("About to produce");
            let producer_delivery_status = producer
                .send(
                    FutureRecord::to(&topic)
                        .payload(&msg)
                        .key(&key),
                    Duration::from_secs(60),
                )
                .await;
            info!("Data produced");
            // We won't record the data in postgres if it fails to send
            // This will mean we will try to send the same data when we query postgres for unsent data
            match producer_delivery_status {
                Ok(delivery) => {
                    assert_sometimes!(true, "Produced data to Kafka", &json!({"result": delivery}));
                    info!("About to postgres");
                    loop {
                        match pg_client.write_produced(bank_account, &mode).await {
                            Ok(_) => { break; },
                            Err(e) => {
                                error!("Failed to send data to postgres {:?}", e);
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                                info!("Going to retry  {:?}", e);
                            }
                        }
                    }
                    pg_client
                        .write_produced(bank_account, &mode)
                        .await
                        .unwrap();
                    info!("Data sent to postgres");
                }
                Err(e) => {
                    error!("Failed to deliver message {:?}", e);
                    assert_sometimes!(false, "Produced data to Kafka", &json!({"error": format!("Failed to deliver message: {:?}", e)}));
                }
            }
        }

        info!("num_bank_accounts_produced {:?}", bank_accounts.len());
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
    info!("{:?}", SystemTime::now().duration_since(UNIX_EPOCH));
    env_logger::init();
    let handle = Handle::current();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mut sig = tokio::signal::unix::signal(SignalKind::user_defined1()).unwrap();
            loop {
                sig.recv().await;
                let dump = handle.dump().await;
                for (i, task) in dump.tasks().iter().enumerate() {
                    let trace = task.trace();
                    info!("TASK {i}:");
                    info!("{trace}\n");
                }
            }
        })
    });

    let producers: Arc<Mutex<HashMap<String, FutureProducer>>> = Arc::new(Mutex::new(HashMap::new()));
    let brokers = vec!["kafka-3:9092", "kafka-2:9092", "kafka-1:9092"];
    let pg_client = Arc::new(Mutex::new(None));

    let state = AppState { producers, brokers, pg_client };
    let app = create_router(state.into());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    info!("Server running at http:{:?}", listener);
    
    axum::serve(listener, app).await.unwrap();    
}