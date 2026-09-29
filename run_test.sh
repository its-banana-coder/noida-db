#!/bin/bash
cargo test --features kafka --lib kafka::tests::idempotence::test_init_producer_id -- --nocapture
