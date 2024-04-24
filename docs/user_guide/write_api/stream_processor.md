---
layout: default
title: Stream Processor
parent: Write APIs
grand_parent: User Guides
permalink: /docs/user_guide/write_api/stream_processor
---
# Stream Processor

Data can be produced to Venice in a nearline fashion, from stream processors. The best supported stream processor is
Apache Samza though we intend to add first-class support for other stream processors in the future. The difference
between using a stream processor library and the [Online Producer](./online_producer.md) library is that a stream 
processor has well-defined semantics around when to ensure that produced data is flushed and a built-in mechanism to 
checkpoint its progress relative to its consumption progress in upstream data sources, whereas the online producer 
library is a lower-level building block which leaves these reliability details up to the user.

For Apache Samza, the integration point is done at the level of the [VeniceSystemProducer](https://github.com/linkedin/venice/blob/main/clients/venice-samza/src/main/java/com/linkedin/venice/samza/VeniceSystemProducer.java)
and [VeniceSystemFactory](https://github.com/linkedin/venice/blob/main/clients/venice-samza/src/main/java/com/linkedin/venice/samza/VeniceSystemFactory.java).

More details to come.