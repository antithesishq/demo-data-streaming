#!/bin/bash
RUST_LOG=error /validator/target/debug/validator --test exactly-once-guarantee
