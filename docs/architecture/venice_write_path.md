---
layout: default
title: Venice Write Path
parent: Venice Architecture
permalink: /docs/architecture/venice_write_path
---

# Introduction

As a high performance derived data storage platform, Venice is designed to ingest large amount of writes from a variety 
of sources while providing low latency read access to the data. These characteristics are achieved by a combination of
design choices and optimizations in the write path. This document describes some internal details of the Venice write path. 

Note that this page is still under construction and only provides glimpses of the Venice writing path. More details will 
be added in the future.

## Venice Store Ingestion on server side

In Venice, a table or the dataset is called a store and each store has multiple partitions. Helix assign leaders and
followers status on each partition to different servers. When assignments are done, each server will create multiple
tasks to handle ingestion for the partitions it's assigned to. The diagram belows describes the data flow of how a 
store ingestion task's created and ingest data on the server side.

![Server Ingestion Diagram](http://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/adamxchen/venice/diagrams/diagrams/uml/server_ingestion.puml)

## Venice Store Ingestion report 

When ingestion is in different stages, the server reports the ingestion status. The ingestion signal is propagated to 
various downstream for different business logics. The diagram below describes the signal flow of the ingestion report.

![Server Ingestion Report Diagram](http://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/adamxchen/venice/diagrams/diagrams/uml/server_ingestion_report.puml)
