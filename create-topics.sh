#!/bin/bash
kafka-topics --create --if-not-exists --replication-factor 3 --topic dc-local.demo-domain.cdc.known-installs.1 --partitions 4 --zookeeper localhost:2181
kafka-topics --create --if-not-exists --replication-factor 3 --topic dc-local.demo-domain.cmd.installation-cmds.1 --partitions 4 --zookeeper localhost:2181
kafka-topics --create --if-not-exists --replication-factor 3 --topic dc-local.demo-domain.fct.vet-api-installs.1 --partitions 4 --zookeeper localhost:2181
kafka-topics --create --if-not-exists --replication-factor 3 --topic dc-local.demo-domain.fct.installation-entity-updates.1 --partitions 4 --zookeeper localhost:2181
