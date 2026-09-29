1. **EngineState**: Add `transactional_producers: HashMap<String, (i64, i16)>` (transactional.id -> (producer_id, producer_epoch)).
2. **InitProducerId**: If `transactional_id` is present, check existing `transactional_producers`.
    - If it exists, bump the epoch. If epoch wraps (reaches max `i16`), bump `producer_id` and reset epoch to 0. (Actually in real Kafka, epoch just increments and can wrap. Let's just bump).
    - If it doesn't exist, assign a new `producer_id` and epoch 0.
    - Return the new `producer_id` and `producer_epoch`.
3. **PartitionState**: Add tracking for transactions:
    - `active_transactions: BTreeMap<i64, i64>` (producer_id -> first_offset). (Note: can use this to compute LSO).
    - `aborted_transactions: Vec<(i64, i64)>` (producer_id, first_offset). (Keep it simple, maybe need `first_offset`). Wait, `AbortedTransaction` has `producer_id` and `first_offset`. So storing `(producer_id, first_offset)` is enough.
    - Let's compute `last_stable_offset` dynamically as `min(active_transactions.values().min().unwrap_or(high_watermark), high_watermark)`.
4. **Produce**:
    - When a producer produces transactional messages (we can know it if `is_transactional` bit is set in batch, but since we don't parse attributes yet, maybe we just assume any batch with `producer_id` >= 0 is tracked? Wait, we can look at the record batch's `attributes`. `is_transactional` is bit 4 of `attributes`.
    - Wait, do we need to check if the batch is a control batch? Control batches have `is_control_batch` set (bit 5).
    - In `handle_produce`, we parse attributes (at offset 17-19 of the v2 batch, 2 bytes). Let's see: `attributes = i16::from_be_bytes(records[17..19])`.
    - If `producer_id` >= 0 and `is_transactional` (attributes & 0x10 != 0), register `active_transactions.entry(producer_id).or_insert(base_offset)`.
    - Fence check: If `producer_epoch` < registered epoch for this `transactional.id`, reject with `INVALID_PRODUCER_EPOCH` (47). But wait, `ProduceRequest` doesn't have `transactional.id`, only `producer_id` and `producer_epoch`. We can just store `producer_epochs: HashMap<i64, i16>` mapping `producer_id` to its current expected epoch! So if `produce` has an older epoch, reject.
5. **EndTxn**:
    - Fencing check for epoch.
    - For each partition the transaction touched (maybe we need `active_transactions` to track this, or we can just scan all partitions and see where this `producer_id` has an active transaction? Wait, Kafka's EndTxn doesn't specify partitions. Wait, `EndTxn` is sent to the transaction coordinator, which then sends `WriteTxnMarker` to partitions. But in our single-node mock, maybe `EndTxn` request itself doesn't have partitions. Ah, Kafka clients send `AddPartitionsToTxn` *first*, and the coordinator remembers them.
    - We need `transactional_partitions: HashMap<String, HashSet<(String, i32)>>` in `EngineState` mapping `transactional_id` to the topics/partitions added in this transaction.
    - In `handle_add_partitions_to_txn`:
        - Do fencing check.
        - Add partitions to `transactional_partitions`.
    - In `handle_end_txn`:
        - Fencing check.
        - For each partition in `transactional_partitions` for this tx:
            - If aborting, add to `aborted_transactions` of that partition (with `first_offset` from `active_transactions`).
            - Remove from `active_transactions`.
            - Clear `transactional_partitions`.
            - (Optional: append a control batch to advance high watermark? Real Kafka writes a control batch. If we just advance `high_watermark` or just remove it, is that enough? Wait, `FetchResponse` needs to return LSO. The mock doesn't need to actually write a control batch unless `read_uncommitted` clients expect to see it. The spec says "EndTxn (commit or abort) appends a control batch (a special record batch with the control bit set) to every partition the transaction touched, and advances the LSO past it.")
            - Okay, append a dummy 61-byte control batch!
6. **Fetch**:
    - If `isolation_level == 1` (`read_committed`):
        - `LSO = min(active_transactions.values(), high_watermark)`.
        - Filter `part_res.records` to only include batches fully below LSO. (Actually, just limit `high_watermark` to LSO? The client fetches up to `high_watermark`. If we set `part_res.high_watermark = LSO`, the client will fetch up to LSO. And we only return records if their offset is < LSO.)
        - Add `aborted_transactions` that have `first_offset < LSO`.
