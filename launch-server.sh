#!/bin/bash

./target/debug/tikv-server --pd-endpoints="127.0.0.1:2379" --addr="127.0.0.1:20160" --data-dir=tikv1 --log-file=tikv1.log &
#./target/debug/tikv-server --pd-endpoints="127.0.0.1:2379" --addr="127.0.0.1:20161" --data-dir=tikv2 --log-file=tikv2.log &
#./target/debug/tikv-server --pd-endpoints="127.0.0.1:2379" --addr="127.0.0.1:20162" --data-dir=tikv3 --log-file=tikv3.log &

echo "Servers Launched"