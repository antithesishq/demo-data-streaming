//! Antithesis driver for the data-streaming demo.
//!
//! The system under test is the bank pipeline: records land in the
//! state-tracker (Postgres), the producer service moves them to Kafka under
//! one of four delivery modes, and the consumer service reads them back and
//! applies them to account balances in DynamoDB.
//!
//! This driver plays the upstream that writes records into the state-tracker
//! and the scheduler that calls the producer and consumer, and it checks the
//! pipeline's guarantees against the state-tracker and DynamoDB. Every random
//! choice comes from hegel, so Antithesis controls it.

use antithesis_instrumentation as _;

use antithesis_sdk::prelude::*;
use hegel::TestCase;
use hegel::generators as gs;
use hegel::stateful::machine;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const INITIAL_FUNDING: i64 = 1000;
/// The producer and the consumer each serve one request at a time (their
/// handlers hold a service-wide lock), so a few outstanding requests are
/// enough to keep them busy.
const MAX_IN_FLIGHT: usize = 2;
/// How long a rule waits for its request before leaving it to finish in the
/// background. Keeps rounds short.
const WAIT_FOR_OUTCOME: Duration = Duration::from_secs(1);
/// How long a background request may take before the driver gives up on it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
/// How long a rule pauses when it has nothing to do (a service is busy, the
/// backlog is full), so a timeline whose services are stuck doesn't spin.
const IDLE: Duration = Duration::from_millis(200);
/// Stop inserting records while this many are still waiting to be produced.
const MAX_BACKLOG: i64 = 400;
const MAX_ACCOUNTS: usize = 300;
/// The window the old harness gave the consumer to drain after production stopped.
const DRAIN_WINDOW: Duration = Duration::from_secs(30);
const RECOVERY_DEADLINE: Duration = Duration::from_secs(240);

#[derive(Clone, Copy, Debug)]
struct ProduceMode {
    route: &'static str,
    topic: &'static str,
}

const PRODUCE_MODES: [ProduceMode; 4] = [
    ProduceMode { route: "exactly_once_single", topic: "bankEO" },
    ProduceMode { route: "exactly_once_batch", topic: "bankEO" },
    ProduceMode { route: "atleast_once_single", topic: "bankAO" },
    ProduceMode { route: "atleast_once_batch", topic: "bankAO" },
];

#[derive(Clone, Copy, Debug)]
struct ConsumeMode {
    route: &'static str,
    topic: &'static str,
    /// The `producer_type` values of the records that land on this topic.
    producer_types: [&'static str; 2],
    idx: usize,
}

const CONSUME_MODES: [ConsumeMode; 2] = [
    ConsumeMode {
        route: "exactly_once_pass_through_consumer",
        topic: "bankEO",
        producer_types: ["/exactly_once_single", "/exactly_once_batch"],
        idx: 0,
    },
    ConsumeMode {
        route: "atleast_once_pass_through_consumer",
        topic: "bankAO",
        producer_types: ["/atleast_once_single", "/atleast_once_batch"],
        idx: 1,
    },
];

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

struct Config {
    producer_url: String,
    consumer_url: String,
    ddb_url: String,
    pg: String,
    kafka: Vec<String>,
}

impl Config {
    fn from_env() -> Self {
        Config {
            producer_url: env_or("PRODUCER_URL", "http://producer:3000"),
            consumer_url: env_or("CONSUMER_URL", "http://consumer:3000"),
            ddb_url: env_or("DDB_URL", "http://ddb:8000"),
            pg: env_or(
                "PG_CONN",
                "host=state-tracker user=u password=p dbname=d connect_timeout=3",
            ),
            kafka: env_or("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
                .split(',')
                .map(str::to_string)
                .collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

/// Postgres connections to the state-tracker, one per worker at a time.
struct Pg {
    conn: String,
    pool: Mutex<Vec<postgres::Client>>,
}

impl Pg {
    fn with<T>(
        &self,
        f: impl FnOnce(&mut postgres::Client) -> Result<T, postgres::Error>,
    ) -> Result<T, String> {
        let pooled = self.pool.lock().unwrap_or_else(|e| e.into_inner()).pop();
        let mut client = match pooled {
            Some(c) => c,
            None => {
                let mut c = postgres::Client::connect(&self.conn, postgres::NoTls)
                    .map_err(|e| format!("connect: {e}"))?;
                c.batch_execute("SET statement_timeout = 10000")
                    .map_err(|e| format!("set statement_timeout: {e}"))?;
                c
            }
        };
        match f(&mut client) {
            Ok(v) => {
                self.pool.lock().unwrap_or_else(|e| e.into_inner()).push(client);
                Ok(v)
            }
            // Drop the connection: it may be broken.
            Err(e) => Err(e.to_string()),
        }
    }
}

fn agent(timeout: Duration) -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(2))
        .timeout(timeout)
        .build()
}

#[derive(Debug, Clone)]
enum Outcome {
    /// The service answered 2xx: the request ran to completion.
    Ok,
    /// The service answered with an error status.
    Failed(String),
    /// No answer: the request may or may not have run, and may still be running.
    Unknown(String),
}

fn post(agent: &ureq::Agent, url: &str) -> Outcome {
    match agent.post(url).set("Content-Type", "application/json").call() {
        Ok(_) => Outcome::Ok,
        Err(ureq::Error::Status(code, resp)) => {
            Outcome::Failed(format!("{code} {}", resp.into_string().unwrap_or_default()))
        }
        Err(e) => Outcome::Unknown(e.to_string()),
    }
}

/// DynamoDB's JSON API. dynamodb-local checks that a signature is present,
/// not that it is valid.
fn ddb_call(agent: &ureq::Agent, url: &str, target: &str, body: &Value) -> Result<Value, String> {
    let resp = agent
        .post(url)
        .set("Content-Type", "application/x-amz-json-1.0")
        .set("X-Amz-Target", &format!("DynamoDB_20120810.{target}"))
        .set(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=dummy/20250101/us-east-1/dynamodb/aws4_request, \
             SignedHeaders=content-type;host;x-amz-target, Signature=0",
        )
        .send_string(&body.to_string());
    match resp {
        Ok(r) => r
            .into_string()
            .map_err(|e| e.to_string())
            .and_then(|s| serde_json::from_str(&s).map_err(|e| e.to_string())),
        Err(ureq::Error::Status(code, r)) => {
            Err(format!("{code} {}", r.into_string().unwrap_or_default()))
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Every account in DynamoDB's `accounts` table, as (iban, balance).
/// `None` when the table doesn't exist yet: nothing was consumed.
fn ddb_scan(agent: &ureq::Agent, url: &str) -> Result<Option<BTreeMap<String, f64>>, String> {
    let mut accounts = BTreeMap::new();
    let mut start: Option<Value> = None;
    loop {
        let mut body = json!({"TableName": "accounts", "ConsistentRead": true});
        if let Some(k) = &start {
            body["ExclusiveStartKey"] = k.clone();
        }
        let page = match ddb_call(agent, url, "Scan", &body) {
            Ok(p) => p,
            Err(e) if e.contains("ResourceNotFoundException") => return Ok(None),
            Err(e) => return Err(e),
        };
        for item in page["Items"].as_array().cloned().unwrap_or_default() {
            let iban = item["iban"]["S"].as_str().unwrap_or_default().to_string();
            let balance = item["balance"]["N"]
                .as_str()
                .and_then(|n| n.parse::<f64>().ok())
                .unwrap_or(f64::NAN);
            accounts.insert(iban, balance);
        }
        match page.get("LastEvaluatedKey") {
            Some(k) if !k.is_null() => start = Some(k.clone()),
            _ => return Ok(Some(accounts)),
        }
    }
}

fn tcp_up(addr: &str) -> Result<(), String> {
    let sock = addr
        .to_socket_addrs()
        .map_err(|e| format!("resolve {addr}: {e}"))?
        .next()
        .ok_or(format!("resolve {addr}: no address"))?;
    TcpStream::connect_timeout(&sock, Duration::from_secs(2))
        .map(|_| ())
        .map_err(|e| format!("{addr}: {e}"))
}

// ---------------------------------------------------------------------------
// The driver's state
// ---------------------------------------------------------------------------

struct Shared {
    cfg: Config,
    pg: Pg,
    /// Short-timeout agent for checks and readiness.
    quick: ureq::Agent,
    /// Long-timeout agent for producer and consumer calls.
    slow: ureq::Agent,
    /// IBANs of the fund-account records the driver inserted.
    accounts: Mutex<Vec<String>>,
    next_account: AtomicI64,
    produce_in_flight: AtomicUsize,
    consume_in_flight: AtomicUsize,
    /// Records requested by consume calls still outstanding, per topic, so
    /// concurrent calls don't ask for the same records twice.
    consume_requested: [AtomicI64; 2],
    /// Set by an eventually: no more producing, inserting or consuming from rules.
    quiescing: AtomicBool,
    eventually: Mutex<()>,
    eventually_calls: AtomicI64,
    started: Instant,
}

static SHARED: OnceLock<Arc<Shared>> = OnceLock::new();

fn log(msg: impl AsRef<str>) {
    println!("driver: {}", msg.as_ref());
}

/// Prints a waiting message only when it changes, so waits don't flood the log.
struct WaitLog(String);
impl WaitLog {
    fn say(&mut self, msg: String) {
        if msg != self.0 {
            log(&msg);
            self.0 = msg;
        }
    }
}

impl Shared {
    // ---- readiness ---------------------------------------------------------

    /// What isn't serving yet; empty when everything is.
    fn not_ready(&self) -> Vec<String> {
        let mut down = vec![];
        if let Err(e) = self.pg.with(|c| c.simple_query("SELECT 1").map(|_| ())) {
            down.push(format!("state-tracker: {e}"));
        }
        if let Err(e) = ddb_call(&self.quick, &self.cfg.ddb_url, "ListTables", &json!({})) {
            down.push(format!("ddb: {e}"));
        }
        // The services have no health endpoint; any HTTP answer, such as a
        // 404 for `/`, means they serve.
        for (name, url) in [("producer", &self.cfg.producer_url), ("consumer", &self.cfg.consumer_url)] {
            match self.quick.get(url).call() {
                Ok(_) | Err(ureq::Error::Status(..)) => {}
                Err(e) => down.push(format!("{name}: {e}")),
            }
        }
        for b in &self.cfg.kafka {
            if let Err(e) = tcp_up(b) {
                down.push(format!("kafka {e}"));
            }
        }
        down
    }

    fn wait_ready(&self, deadline: Option<Instant>) -> Vec<String> {
        let mut wl = WaitLog(String::new());
        loop {
            let down = self.not_ready();
            if down.is_empty() {
                log("every service answers");
                return down;
            }
            wl.say(format!("waiting for: {}", down.join("; ")));
            if deadline.is_some_and(|d| Instant::now() > d) {
                return down;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    fn create_schema(&self) {
        let mut wl = WaitLog(String::new());
        loop {
            let r = self.pg.with(|c| {
                c.batch_execute(
                    "CREATE TABLE IF NOT EXISTS producer (
                        id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        data TEXT,
                        topic VARCHAR(255) DEFAULT NULL,
                        produced_timestamp TIMESTAMP DEFAULT NULL,
                        producer_type VARCHAR(255) DEFAULT NULL,
                        consumed_timestamp TIMESTAMP DEFAULT NULL,
                        consumer_type VARCHAR(255) DEFAULT NULL,
                        consumed_count INT DEFAULT 0
                    )",
                )
            });
            match r {
                Ok(()) => {
                    log("state-tracker schema ready");
                    return;
                }
                Err(e) => wl.say(format!("creating the producer table: {e}")),
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    // ---- state-tracker queries ---------------------------------------------

    fn backlog(&self) -> Result<i64, String> {
        self.pg.with(|c| {
            c.query_one("SELECT count(*) FROM producer WHERE produced_timestamp IS NULL", &[])
                .map(|r| r.get::<_, i64>(0))
        })
    }

    /// Records produced onto `mode`'s topic and not yet recorded as consumed.
    fn unconsumed(&self, mode: &ConsumeMode) -> Result<i64, String> {
        let types: Vec<&str> = mode.producer_types.to_vec();
        self.pg.with(|c| {
            c.query_one(
                "SELECT count(*) FROM producer
                 WHERE producer_type = ANY($1) AND consumed_timestamp IS NULL",
                &[&types],
            )
            .map(|r| r.get::<_, i64>(0))
        })
    }

    fn insert(&self, rows: &[(Value, &'static str)]) -> Result<(), String> {
        self.pg.with(|c| {
            let mut tx = c.transaction()?;
            for (data, topic) in rows {
                tx.execute(
                    "INSERT INTO producer (data, topic) VALUES ($1, $2)",
                    &[&data.to_string(), topic],
                )?;
            }
            tx.commit()
        })
    }

    // ---- background requests -----------------------------------------------

    /// Sends `url` on a background thread and waits up to WAIT_FOR_OUTCOME for
    /// its outcome. `done` runs when the outcome arrives, however late.
    fn send(
        self: &Arc<Self>,
        url: String,
        done: impl FnOnce(&Shared, &Outcome) + Send + 'static,
    ) -> Option<Outcome> {
        let (tx, rx) = mpsc::channel();
        let me = self.clone();
        std::thread::spawn(move || {
            let outcome = post(&me.slow, &url);
            done(&me, &outcome);
            let _ = tx.send(outcome);
        });
        rx.recv_timeout(WAIT_FOR_OUTCOME).ok()
    }

    fn produce(self: &Arc<Self>, mode: ProduceMode, n: i64) -> Option<Outcome> {
        if self.produce_in_flight.fetch_add(1, Ordering::SeqCst) >= MAX_IN_FLIGHT {
            self.produce_in_flight.fetch_sub(1, Ordering::SeqCst);
            std::thread::sleep(IDLE);
            return None;
        }
        let url = format!("{}/{}/{}/{}", self.cfg.producer_url, mode.route, mode.topic, n);
        log(format!("produce: sending {} {} {n}", mode.route, mode.topic));
        self.send(url, move |s, outcome| {
            s.produce_in_flight.fetch_sub(1, Ordering::SeqCst);
            log(format!("produce: {} {} {n}: {outcome:?}", mode.route, mode.topic));
            let ok = matches!(outcome, Outcome::Ok);
            let details = json!({"mode": mode.route, "records": n, "outcome": format!("{outcome:?}")});
            match mode.route {
                "exactly_once_single" => assert_sometimes!(ok, "a produce request completes: exactly_once_single", &details),
                "exactly_once_batch" => assert_sometimes!(ok, "a produce request completes: exactly_once_batch", &details),
                "atleast_once_single" => assert_sometimes!(ok, "a produce request completes: atleast_once_single", &details),
                _ => assert_sometimes!(ok, "a produce request completes: atleast_once_batch", &details),
            }
            assert_sometimes!(!ok, "a produce request fails or gets no answer", &details);
        })
    }

    fn consume(self: &Arc<Self>, mode: ConsumeMode, n: i64) -> Option<Outcome> {
        if self.consume_in_flight.fetch_add(1, Ordering::SeqCst) >= MAX_IN_FLIGHT {
            self.consume_in_flight.fetch_sub(1, Ordering::SeqCst);
            std::thread::sleep(IDLE);
            return None;
        }
        self.consume_requested[mode.idx].fetch_add(n, Ordering::SeqCst);
        let url = format!("{}/{}/{}/{}", self.cfg.consumer_url, mode.route, mode.topic, n);
        log(format!("consume: sending {} {} {n}", mode.route, mode.topic));
        self.send(url, move |s, outcome| {
            log(format!("consume: {} {} {n}: {outcome:?}", mode.route, mode.topic));
            s.consume_in_flight.fetch_sub(1, Ordering::SeqCst);
            s.consume_requested[mode.idx].fetch_sub(n, Ordering::SeqCst);
            let ok = matches!(outcome, Outcome::Ok);
            let details = json!({"mode": mode.route, "records": n, "outcome": format!("{outcome:?}")});
            if mode.idx == 0 {
                assert_sometimes!(ok, "a consume request completes: exactly_once_pass_through_consumer", &details);
            } else {
                assert_sometimes!(ok, "a consume request completes: atleast_once_pass_through_consumer", &details);
            }
            assert_sometimes!(!ok, "a consume request fails or gets no answer", &details);
        })
    }

    // ---- guarantee checks --------------------------------------------------
    // These keep the old harness's assertion messages, so the properties line
    // up with the history of earlier runs.

    /// No record produced by an exactly-once producer is consumed twice.
    fn check_exactly_once(&self) {
        let rows = self.pg.with(|c| {
            c.query(
                "SELECT id, producer_type, consumer_type, consumed_count FROM producer
                 WHERE consumed_count > 1 AND producer_type LIKE '%exactly_once%'
                 ORDER BY id LIMIT 50",
                &[],
            )
        });
        let Ok(rows) = rows else { return };
        let dupes: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({"id": r.get::<_, i32>(0), "producer_type": r.get::<_, Option<String>>(1),
                       "consumer_type": r.get::<_, Option<String>>(2), "consumed_count": r.get::<_, Option<i32>>(3)})
            })
            .collect();
        assert_always!(
            dupes.is_empty(),
            "Instances of exactly_once_producer produces more than once",
            &json!({"result": dupes})
        );
    }

    /// The consumed records form one contiguous run of ids: consumption
    /// follows generation order, with no record skipped.
    fn check_order(&self) {
        let ids = self.pg.with(|c| {
            c.query("SELECT id FROM producer WHERE consumed_timestamp IS NOT NULL ORDER BY id", &[])
        });
        let Ok(ids) = ids else { return };
        let ids: Vec<i32> = ids.iter().map(|r| r.get(0)).collect();
        let gaps: Vec<Value> = ids
            .windows(2)
            .filter(|w| w[0] + 1 != w[1])
            .take(20)
            .map(|w| json!({"after": w[0], "next_consumed": w[1]}))
            .collect();
        assert_always!(
            gaps.is_empty(),
            "Consumed data is in the same order as data generated by data_generator",
            &json!({"consumed": ids.len(), "first_gaps": gaps})
        );
    }

    /// Two identical consecutive scans. A scan isn't isolated from
    /// transactions, so one scan can see half of a transfer; two identical
    /// ones can't, since the second reads every item after the first ended.
    fn stable_scan(&self) -> Option<BTreeMap<String, f64>> {
        let mut prev = ddb_scan(&self.quick, &self.cfg.ddb_url).ok()??;
        for _ in 0..5 {
            let next = ddb_scan(&self.quick, &self.cfg.ddb_url).ok()??;
            if next == prev {
                return Some(next);
            }
            prev = next;
        }
        None
    }

    /// Transfers move money between accounts and never create or destroy it:
    /// the total balance is the initial funding of every account.
    fn check_balance(&self) {
        let Some(accounts) = self.stable_scan() else { return };
        let total: f64 = accounts.values().sum();
        let expected = (accounts.len() as i64 * INITIAL_FUNDING) as f64;
        assert_always!(
            total == expected,
            "Final total equals expected total",
            &json!({"finalTotal": total.to_string(), "expectedTotal": expected.to_string(),
                    "accounts": accounts.len()})
        );
    }

    /// Every account in DynamoDB comes from a fund-account record that the
    /// state-tracker shows as consumed. DynamoDB is read first: the consumer
    /// records a record as consumed before it applies it.
    fn check_ibans(&self) {
        let Ok(Some(accounts)) = ddb_scan(&self.quick, &self.cfg.ddb_url) else { return };
        let funded = self.pg.with(|c| {
            c.query(
                "SELECT data::jsonb->>'iban' FROM producer
                 WHERE consumed_timestamp IS NOT NULL AND topic = '_banktest_fund_account'",
                &[],
            )
        });
        let Ok(funded) = funded else { return };
        let funded: HashSet<String> = funded.iter().filter_map(|r| r.get(0)).collect();
        let diff: Vec<&String> = accounts.keys().filter(|i| !funded.contains(*i)).collect();
        assert_always!(
            diff.is_empty(),
            "produced ibans match ibans in dynamo",
            &json!({"diff": diff.iter().take(50).collect::<Vec<_>>(), "count": diff.len()})
        );
    }

    fn produced_not_consumed(&self) -> Result<Vec<(i32, Option<String>)>, String> {
        self.pg.with(|c| {
            c.query(
                "SELECT id, producer_type FROM producer
                 WHERE produced_timestamp IS NOT NULL AND consumed_timestamp IS NULL ORDER BY id",
                &[],
            )
            .map(|rows| rows.iter().map(|r| (r.get(0), r.get(1))).collect())
        })
    }

    // ---- eventually --------------------------------------------------------

    fn eventually(self: &Arc<Self>) {
        let Ok(_guard) = self.eventually.try_lock() else { return };
        self.quiescing.store(true, Ordering::SeqCst);
        log(format!("eventually: starting after {:?} of workload", self.started.elapsed()));

        if let Ok(stop) = std::env::var("ANTITHESIS_STOP_FAULTS") {
            match std::process::Command::new(&stop).arg("3600").status() {
                Ok(s) => log(format!("eventually: stopped faults ({s})")),
                Err(e) => log(format!("eventually: {stop} failed: {e}")),
            }
        }

        let down = self.wait_ready(Some(Instant::now() + RECOVERY_DEADLINE));
        log("eventually: assertions start");
        assert_always!(
            down.is_empty(),
            "the system recovers once faults stop",
            &json!({"not_serving": down})
        );
        if down.is_empty() {
            // Production stopped: drain both topics, then check that
            // everything produced was consumed.
            let start = Instant::now();
            while start.elapsed() < DRAIN_WINDOW {
                let mut remaining = 0;
                for mode in CONSUME_MODES {
                    let Ok(left) = self.unconsumed(&mode) else { continue };
                    remaining += left;
                    let ask = (left - self.consume_requested[mode.idx].load(Ordering::SeqCst)).min(100);
                    if ask > 0 && self.consume(mode, ask).is_none()
                        && self.consume_in_flight.load(Ordering::SeqCst) >= MAX_IN_FLIGHT
                    {
                        log(format!(
                            "eventually: drain of {} waits: {MAX_IN_FLIGHT} consume calls still outstanding",
                            mode.topic
                        ));
                    }
                }
                if remaining == 0 {
                    break;
                }
                std::thread::sleep(Duration::from_secs(1));
            }
            if let Ok(missing) = self.produced_not_consumed() {
                assert_always!(
                    missing.is_empty(),
                    "Produced data matches consumed data after 30s of not producing and only consuming",
                    &json!({"count": missing.len(),
                            "first": missing.iter().take(50).map(|(id, t)| json!({"id": id, "producer_type": t})).collect::<Vec<_>>()})
                );
            }
            self.check_exactly_once();
            self.check_order();
            self.check_balance();
            self.check_ibans();
        }
        log("eventually: ending the timeline");
        std::process::exit(0);
    }
}

// ---------------------------------------------------------------------------
// The state machine
// ---------------------------------------------------------------------------

struct Driver {
    s: Arc<Shared>,
    eventually_enabled: bool,
}

fn digits(tc: &TestCase, n: usize) -> String {
    (0..n)
        .map(|_| char::from(b'0' + tc.draw(gs::integers::<u8>().max_value(9))))
        .collect()
}

fn letters(tc: &TestCase, n: usize) -> String {
    (0..n)
        .map(|_| char::from(b'A' + tc.draw(gs::integers::<u8>().max_value(25))))
        .collect()
}

#[hegel::concurrent_state_machine]
impl Driver {
    /// New accounts, each funded with INITIAL_FUNDING once consumed.
    #[rule(weight = 2)]
    fn insert_accounts(&self, tc: TestCase) {
        let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(10));
        if self.s.quiescing.load(Ordering::SeqCst)
            || self.s.accounts.lock().unwrap_or_else(|e| e.into_inner()).len() >= MAX_ACCOUNTS
            || self.s.backlog().map_or(true, |b| b > MAX_BACKLOG)
        {
            return std::thread::sleep(IDLE);
        }
        let mut rows = vec![];
        let mut ibans = vec![];
        for _ in 0..n {
            // Unique by construction: the counter fills the account number.
            let k = self.s.next_account.fetch_add(1, Ordering::SeqCst);
            let iban = format!("GB{}{}{:014}", digits(&tc, 2), letters(&tc, 4), k);
            let data = json!({
                "iban": iban,
                "aba": digits(&tc, 9),
                "swift11": format!("{}GB{}XXX", letters(&tc, 4), letters(&tc, 2)),
                "bank_country": "GB",
                "amount": INITIAL_FUNDING,
            });
            rows.push((data, "_banktest_fund_account"));
            ibans.push(iban);
        }
        if self.s.insert(&rows).is_ok() {
            self.s.accounts.lock().unwrap_or_else(|e| e.into_inner()).extend(ibans);
        }
    }

    /// Transfers between known accounts, including ones whose funding
    /// record hasn't been produced or consumed yet.
    #[rule(weight = 3)]
    fn insert_transfers(&self, tc: TestCase) {
        let accounts = self.s.accounts.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if accounts.is_empty()
            || self.s.quiescing.load(Ordering::SeqCst)
            || self.s.backlog().map_or(true, |b| b > MAX_BACKLOG)
        {
            return std::thread::sleep(IDLE);
        }
        let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(20));
        let pick = |tc: &TestCase| -> String {
            // Favor the newest accounts sometimes: their funding is the most
            // likely to still be in flight.
            let recent = tc.draw(gs::weighted_booleans(0.3));
            let lo = if recent { accounts.len().saturating_sub(10) } else { 0 };
            let i = tc.draw(gs::integers::<usize>().min_value(lo).max_value(accounts.len() - 1));
            accounts[i].clone()
        };
        let rows: Vec<(Value, &'static str)> = (0..n)
            .map(|_| {
                let from = pick(&tc);
                let to = pick(&tc);
                let amount = tc.draw(gs::integers::<i64>().min_value(0).max_value(99));
                (json!({"from": from, "to": to, "amount": amount}), "_banktest_transaction")
            })
            .collect();
        let _ = self.s.insert(&rows);
    }

    #[rule(weight = 4)]
    fn produce(&self, tc: TestCase) {
        let mode = tc.draw(gs::sampled_from(PRODUCE_MODES.to_vec()));
        let n = tc.draw(gs::integers::<i64>().min_value(1).max_value(100));
        if self.s.quiescing.load(Ordering::SeqCst) {
            return;
        }
        if let Some(o) = self.s.produce(mode, n) {
            tc.note(&format!("produce {} {n}: {o:?}", mode.route));
        }
    }

    #[rule(weight = 4)]
    fn consume(&self, tc: TestCase) {
        let mode = tc.draw(gs::sampled_from(CONSUME_MODES.to_vec()));
        // A consume call blocks until it has read as many records as it asked
        // for, holding the consumer's only lock, so asking for more than the
        // topic holds parks the consumer for good (FINDINGS.md). Ask only for
        // records the state-tracker shows as produced and not yet consumed.
        if self.s.quiescing.load(Ordering::SeqCst) {
            return;
        }
        let left = match self.s.unconsumed(&mode) {
            Ok(l) => l,
            Err(e) => return log(format!("consume: counting unconsumed records: {e}")),
        };
        let avail = left - self.s.consume_requested[mode.idx].load(Ordering::SeqCst);
        if avail <= 0 {
            return std::thread::sleep(IDLE);
        }
        let n = tc.draw(gs::integers::<i64>().min_value(1).max_value(avail.min(100)));
        if let Some(o) = self.s.consume(mode, n) {
            tc.note(&format!("consume {} {n}: {o:?}", mode.route));
        }
    }

    #[rule]
    fn check_exactly_once(&self, _: TestCase) {
        self.s.check_exactly_once();
    }

    #[rule]
    fn check_order(&self, _: TestCase) {
        self.s.check_order();
    }

    #[rule]
    fn check_balance(&self, _: TestCase) {
        self.s.check_balance();
    }

    #[rule]
    fn check_ibans(&self, _: TestCase) {
        self.s.check_ibans();
    }

    #[rule]
    fn eventually(&self, tc: TestCase) {
        let calls = self.s.eventually_calls.fetch_add(1, Ordering::Relaxed) + 1;
        if calls % 1000 == 0 {
            log(format!("{calls} eventually-rule calls after {:?}", self.s.started.elapsed()));
        }
        if self.eventually_enabled && tc.draw(gs::weighted_booleans(0.001)) {
            self.s.eventually();
        }
    }
}

#[hegel::main(phases = [hegel::Phase::Generate])]
fn main(tc: TestCase) {
    antithesis_init();
    let s = SHARED
        .get_or_init(|| {
            let cfg = Config::from_env();
            Arc::new(Shared {
                pg: Pg { conn: cfg.pg.clone(), pool: Mutex::new(vec![]) },
                cfg,
                quick: agent(Duration::from_secs(5)),
                slow: agent(REQUEST_TIMEOUT),
                accounts: Mutex::new(vec![]),
                next_account: AtomicI64::new(0),
                produce_in_flight: AtomicUsize::new(0),
                consume_in_flight: AtomicUsize::new(0),
                consume_requested: [AtomicI64::new(0), AtomicI64::new(0)],
                quiescing: AtomicBool::new(false),
                eventually: Mutex::new(()),
                eventually_calls: AtomicI64::new(0),
                started: Instant::now(),
            })
        })
        .clone();

    // Startup: every node answers, then admin setup, then setup complete.
    s.wait_ready(None);
    s.create_schema();
    lifecycle::setup_complete(&json!({"message": "data-streaming pipeline ready"}));
    log("setup complete; waiting for /workload-active");
    while !std::path::Path::new("/workload-active").exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_reachable!("the workload starts", &json!({}));
    log("workload active");

    // Local loop: EVENTUALLY_DISABLED turns the eventually rule off, and
    // EVENTUALLY_AFTER_SECS runs one after that many seconds of workload.
    let eventually_enabled = std::env::var("EVENTUALLY_DISABLED").map_or(true, |v| v.is_empty());
    if let Some(secs) = std::env::var("EVENTUALLY_AFTER_SECS").ok().and_then(|v| v.parse().ok()) {
        let s2 = s.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(secs));
            s2.eventually();
        });
    }

    machine(Driver { s, eventually_enabled })
        .steps(i64::MAX)
        .min_concurrency(2)
        .max_concurrency(6)
        .run_concurrent(tc);
}
