#!/bin/bash

./tidb-v5.0.1-linux-amd64/bin/pd-server --name=pd1 --data-dir=pd1 --client-urls="http://127.0.0.1:2379" --peer-urls="http://127.0.0.1:2380" --initial-cluster="pd1=http://127.0.0.1:2380" --log-file=pd1.log &