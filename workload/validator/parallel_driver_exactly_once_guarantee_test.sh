#!/bin/bash
RUST_LOG=debug /validator/target/debug/validator --test exactly-once-guarantee
