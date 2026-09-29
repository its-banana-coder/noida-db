sed -i -e '/pub producer_seqs: HashMap<(i64, i16), (i32, i64)>,/a\    pub active_txns: HashMap<i64, i64>,\n    pub aborted_txns: Vec<(i64, i64)>,' src/kafka/engine.rs
sed -i -e '/producer_seqs: HashMap::new(),/a\            active_txns: HashMap::new(),\n            aborted_txns: Vec::new(),' src/kafka/engine.rs
sed -i -e '/pub clock: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,/a\    pub producer_epochs: HashMap<i64, i16>,\n    pub txn_producers: HashMap<String, (i64, i16)>,\n    pub txn_partitions: HashMap<String, HashSet<(String, i32)>>,' src/kafka/engine.rs
sed -i -e '/clock: None,/a\            producer_epochs: HashMap::new(),\n            txn_producers: HashMap::new(),\n            txn_partitions: HashMap::new(),' src/kafka/engine.rs
