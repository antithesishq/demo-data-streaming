use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};

use std::fmt::Write;

use log::{info, error, debug, warn};
use env_logger;

use reqwest;

use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
// use std::error::Error;

use std::net::SocketAddr;

use serde::{Serialize, Deserialize};
use serde_json::Value;
use serde_json::json;
use antithesis_sdk::prelude::*;
use serde_postgres::de::from_row;

use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use tokio_postgres::{Client, NoTls, Error, Row};

use postgres_types::{Type, IsNull};
use tokio::time::sleep;
use clap::{Arg, Command, ValueEnum};


// 1. Everything that has been produced has been consumed
//      sql statement to check everything with a producer timestamp has a consumer timestamp
//      make this check 30s long
// 2. Consumption order == Faker Production order: 
//      this can be checked by ensuring the ids of all consumer 
//      timestamps is in monotonic incremental order
// 3. Consumed count == 1 && Producer type == "/exactly_once":
//      trivial sql statement

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    async fn get_produced_data(&self) -> Result<Vec<BankData>, Error> {
        let query = "
            SELECT * 
            FROM producer 
            WHERE produced_timestamp IS NOT NULL 
            ORDER BY id ASC 
        ";

        let rows = self.client
            .query(query, &[])
            .await
            .map_err(|e| {
                error!("Failed to perform query: {}", e);
                //assert_sometimes!(false, "")
                e
            })?;

        let accounts: Vec<BankData> = rows
            .iter()
            .map(BankData::from_row)
            .collect();

        Ok(accounts)
    }

    async fn get_consumed_data(&self) -> Result<Vec<BankData>, Error> {
        let query = "
            SELECT * 
            FROM producer 
            WHERE consumed_timestamp IS NOT NULL 
            ORDER BY id ASC 
        ";

        let rows = self.client
            .query(query, &[])
            .await
            .map_err(|e| {
                error!("Failed to perform query: {}", e);
                e
            })?;

        let accounts: Vec<BankData> = rows
            .iter()
            .map(BankData::from_row)
            .collect();
        
        Ok(accounts)
    }

    async fn check_exactly_once_producer_guarantee(&self) -> Result<Vec<BankData>, Error> {
        let query = "
            SELECT *
            FROM producer
            WHERE consumed_count > 1 AND producer_type LIKE '%exactly_once%'
        ";

        let rows = self.client
            .query(query, &[])
            .await
            .map_err(|e| {
                error!("Failed to perform query: {}", e);
                e
            })?;

        let accounts: Vec<BankData> = rows
            .iter()
            .map(BankData::from_row)
            .collect();

        Ok(accounts)
    }

    async fn check_processor_table_exists(&self) -> bool {
        let query = "
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'processor'
            );
        ";
        match self.client.query_one(query, &[]).await {
            Ok(row) => row.get::<_, bool>(0),
            Err(_) => false,
        }
    }

    async fn get_unprocessed_produced_data(&self) -> Result<Vec<BankData>, Error> {
        // Find records that have been produced to Kafka but not yet processed
        let query = "
            SELECT p.*
            FROM producer p
            LEFT JOIN processor pr ON p.id = pr.id
            WHERE p.produced_timestamp IS NOT NULL
              AND pr.id IS NULL
            ORDER BY p.id ASC
        ";

        let rows = self.client
            .query(query, &[])
            .await
            .map_err(|e| {
                error!("Failed to perform unprocessed data query: {}", e);
                e
            })?;

        let accounts: Vec<BankData> = rows
            .iter()
            .map(BankData::from_row)
            .collect();

        Ok(accounts)
    }

    async fn check_enrichment_risk_scores(&self) -> Result<i64, Error> {
        let query = "
            SELECT COUNT(*) FROM processor
            WHERE risk_score < 0.0 OR risk_score > 1.0
        ";
        let row = self.client.query_one(query, &[]).await?;
        Ok(row.get::<_, i64>(0))
    }

    async fn check_enrichment_categories(&self) -> Result<i64, Error> {
        let query = "
            SELECT COUNT(*) FROM processor
            WHERE transaction_category NOT IN ('fund', 'transfer_self', 'transfer_small', 'transfer_large', 'unknown', 'parse_error')
        ";
        let row = self.client.query_one(query, &[]).await?;
        Ok(row.get::<_, i64>(0))
    }

    async fn check_fund_classification(&self) -> Result<i64, Error> {
        // Fund transactions have an 'iban' field in data; they should be classified as 'fund'
        let query = "
            SELECT COUNT(*) FROM processor pr
            JOIN producer p ON pr.id = p.id
            WHERE p.data LIKE '%\"iban\"%'
              AND p.data NOT LIKE '%\"from\"%'
              AND pr.transaction_category != 'fund'
        ";
        let row = self.client.query_one(query, &[]).await?;
        Ok(row.get::<_, i64>(0))
    }

    async fn check_self_transfer_classification(&self) -> Result<i64, Error> {
        // Self-transfers have the same 'from' and 'to' fields; they should be classified as 'transfer_self'
        // We use a subquery approach since PostgreSQL text LIKE is limited for JSON
        let query = "
            SELECT COUNT(*) FROM processor pr
            JOIN producer p ON pr.id = p.id
            WHERE p.data::jsonb ? 'from'
              AND p.data::jsonb ? 'to'
              AND p.data::jsonb->>'from' = p.data::jsonb->>'to'
              AND pr.transaction_category != 'transfer_self'
        ";
        let row = self.client.query_one(query, &[]).await?;
        Ok(row.get::<_, i64>(0))
    }
}

#[derive(ValueEnum, Clone, Debug)]
enum Tests {
    ExactlyOnceGuarantee,
    ConsumptionOrder,
    ProducerConsumerMatches,
    ProcessorMatches,
    EnrichmentInvariants,
}

fn find_broken_id_sequence(b_as: &[BankData]) -> Vec<BankData> {
    b_as.windows(2)
        .filter_map(|pair| {
            if pair[0].id + 1 != pair[1].id {
                Some(pair[1].clone())
            } else {
                None
            }
        })
        .collect()
}

fn find_produced_data_thats_not_consumed_yet(b_as: &[BankData]) -> Vec<BankData> {
    b_as.iter()
        .filter_map(|b_a| {
            if b_a.produced_timestamp.is_some() && b_a.consumed_timestamp.is_some() {
                None
            } else {
                Some(b_a.clone())
            }
        })
        .collect() 
}

async fn run_processor_check_for_30s(pg_client: &Postgres) -> Option<Vec<BankData>> {
    let start_time = Instant::now();
    let duration = Duration::from_secs(30);
    let mut last_result = None;

    while Instant::now().duration_since(start_time) < duration {
        match pg_client.get_unprocessed_produced_data().await {
            Ok(b_as) => {
                info!("Unprocessed produced data length {:?}", b_as.len());
                last_result = Some(b_as);
            }
            Err(err) => {
                error!("Failed to get unprocessed produced data: {}", err);
            }
        }
        sleep(Duration::from_millis(5000)).await;
    }

    info!("Finished processor check after 30 seconds.");
    last_result
}

async fn run_check_for_30s(pg_client: Postgres) -> Option<Vec<BankData>> {
    let start_time = Instant::now();
    let duration = Duration::from_secs(30);
    let mut last_result = None;

    while Instant::now().duration_since(start_time) < duration {
        match pg_client.get_produced_data().await {
            Ok(b_as) => {
                info!("Produced data length {:?}", b_as.len());
                last_result = Some(find_produced_data_thats_not_consumed_yet(&b_as));
                //info!("Produced data thats not consumed yet {:?}", last_result);
            }
            Err(err) => {
                error!("Failed to get produced data: {}", err);
            }
        }
        sleep(Duration::from_millis(5000)).await;
    }
    
    info!("Finished checking after 30 seconds.");
    last_result
}

#[tokio::main]
async fn main() {
    antithesis_init();
    env_logger::init();

    if let Ok(pg_client) = Postgres::new("host=state-tracker user=u password=p dbname=d ").await {
        info!("Postgres client initialized successfully.");
        let matches = Command::new("Validator CLI")
            .version("1.0")
            .author("ADawg")
            .about("You can use this to launch specific validation logic around Kafka Consumers and Producer")
            .arg(
                Arg::new("test")
                    .short('t')
                    .long("test")
                    .value_parser(clap::builder::EnumValueParser::<Tests>::new())
                    .required(true)
                    .help("Specifies the test to perform: ExactlyOnceGuarantee or ConsumptionOrder or ProducerConsumerMatches"),
            )
            .get_matches();

        let test: Tests = matches.get_one::<Tests>("test").unwrap().to_owned();
        match test {
            Tests::ExactlyOnceGuarantee => {
                match pg_client.check_exactly_once_producer_guarantee().await {
                    Ok(b_as) => {
                        assert_always!(b_as.len() == 0, "Instances of exactly_once_producer produces more than once", &json!({"result": b_as}));
                    },
                    Err(err) => {
                        error!("Failed to check exactly_once guarantee: {}", err);
                    }
                }
            }
            Tests::ConsumptionOrder => {
                match pg_client.get_consumed_data().await {
                    Ok(b_as) => {
                        info!("Consumed data {:?}", b_as);
                        let b_as_broken_sequence = find_broken_id_sequence(&b_as);
                        assert_sometimes!(b_as_broken_sequence.len() == 0, "Consumed data is in the same order as data generated by data_generator", &json!({"result": b_as_broken_sequence}))
                    },
                    Err(err) => {
                        error!("Failed to get consumed data: {}", err);
                    }
                }
            }
            Tests::ProducerConsumerMatches => {
                match run_check_for_30s(pg_client).await {
                    Some(b_as) => {
                        // On the test composer eventually script
                        // Call the consumer with a large number of consumption before calling the validator
                        info!("Check completed");
                        assert_sometimes!(b_as.len() == 0, "Produced data matches consumed data after 30s of not producing and only consuming", &json!({"result": b_as}))
                    },
                    None => {}
                }
            }
            Tests::ProcessorMatches => {
                if !pg_client.check_processor_table_exists().await {
                    info!("Processor table does not exist yet, skipping check.");
                } else {
                    match run_processor_check_for_30s(&pg_client).await {
                        Some(b_as) => {
                            info!("Processor matches check completed");
                            assert_always!(b_as.len() == 0, "All produced data has been processed by the stream processor after 30s", &json!({"result": b_as}))
                        },
                        None => {}
                    }
                }
            }
            Tests::EnrichmentInvariants => {
                if !pg_client.check_processor_table_exists().await {
                    info!("Processor table does not exist yet, skipping check.");
                } else {
                    // Check 1: risk_score in valid range [0.0, 1.0]
                    match pg_client.check_enrichment_risk_scores().await {
                        Ok(count) => {
                            assert_always!(count == 0, "All risk scores are within valid range [0.0, 1.0]", &json!({"violations": count}));
                        },
                        Err(err) => {
                            error!("Failed to check risk scores: {}", err);
                        }
                    }

                    // Check 2: transaction_category from allowed set
                    match pg_client.check_enrichment_categories().await {
                        Ok(count) => {
                            assert_always!(count == 0, "All transaction categories are from the allowed set", &json!({"violations": count}));
                        },
                        Err(err) => {
                            error!("Failed to check categories: {}", err);
                        }
                    }

                    // Check 3: Fund transactions classified as 'fund'
                    match pg_client.check_fund_classification().await {
                        Ok(count) => {
                            assert_always!(count == 0, "Fund transactions are always categorized as fund", &json!({"violations": count}));
                        },
                        Err(err) => {
                            error!("Failed to check fund classification: {}", err);
                        }
                    }

                    // Check 4: Self-transfers classified as 'transfer_self'
                    match pg_client.check_self_transfer_classification().await {
                        Ok(count) => {
                            assert_always!(count == 0, "Self-transfers are always categorized as transfer_self", &json!({"violations": count}));
                        },
                        Err(err) => {
                            error!("Failed to check self-transfer classification: {}", err);
                        }
                    }
                }
            }
        }
    } else {
        error!("Failed to initialize Postgres client.");
    }
    //let action = matches.value_of("action").unwrap();
    // println!("Hello, world!");
}
