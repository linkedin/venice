---
layout: default
title: Merging Batch & Real-Time Data
parent: Design Patterns
grand_parent: User Guides
permalink: /docs/user_guide/design_patterns/merging_batch_and_rt_data
---

# Merging Batch & Real-Time Data

Venice being a derived data platform, an important category of use cases is to merge batch data sources and real-time
data sources. This is a field where the industry has come up with multiple patterns over the past decade and a half.
This page provides an overview of all these patterns, and how they can be implemented in Venice. The patterns are
presented in the order they were published over the years.

While it is useful to understand the history of how things were done in the past and how they have evolved over time, in
practice, most Venice users choose the [Hybrid Store](#hybrid-store) design pattern. Impatient readers may choose to go
directly to that section if they wish to skip the historical context.

## Lambda Architecture

The Lambda Architecture was proposed by [Nathan Marz](https://github.com/nathanmarz) in 2011, in a blog post oddly
titled [How to beat the CAP theorem](http://nathanmarz.com/blog/how-to-beat-the-cap-theorem.html). While the post
describes what has come to be known as the "lambda architecture", it does not actually mention it by that name. Whether
the presented ideas actually "beat" the CAP theorem, or merely side-step it in certain contexts is debatable, but it is
ultimately unimportant to the subject we are interested in.

In a nutshell, the idea is to have two parallel pipelines, one for batch, and one for data. Both of these pipelines are
going to perform processing and serving of their respective data, each using specialized technology for the job. Only at
the periphery are the two pipelines merged together, presumably within the user's own application.

![](../../assets/images/lambda_architecture.drawio.svg)

### Implementing the Lambda Architecture in Venice

The Lambda Architecture, exactly as it is proposed by Nathan Marz, can be implemented using two Venice stores: a 
batch-only one, and a real-time-only one. The application can query both stores and implement whatever arbitrary 
reconciliation logic they wish.

## Kappa Architecture

The Kappa Architecture was proposed by [Jay Kreps](https://github.com/jkreps) in 2014, in a blog post titled
[Questioning the Lambda Architecture](https://www.oreilly.com/radar/questioning-the-lambda-architecture/).

In a nutshell, the idea is that the Lambda Architecture's two parallel pipelines are cumbersome to maintain, especially 
given that they would be implemented in different technologies (some batch processing framework and a stream processing
one). The Kappa Architecture, on the other hand, proposes to use only a stream processor, but to configure it to run
either in "real-time processing" or in "historical reprocessing" mode. Whenever data needs to be reprocessed, a new 
pipeline can be instantiated, configured to start from the beginning of the historical input, output all of its 
processed data into a new instance of a database, and then keep going to process real-time events after that. The 
application can then switch over its read traffic to the new database instance.

![](https://dmgpayxepw99m.cloudfront.net/kappa-61d0afc292912b61ce62517fa2bd4309.png)
### Implementing the Lambda Architecture in Venice

The Kappa Architecture, exactly as it is proposed by Jay Kreps, can again be implemented using two Venice stores, except
that both of them are real-time-only. The user does need to manually keep creating new stores and manually switch over 
the reads to the new one when ready. It does achieve the purpose of maintaining only a stream processing stack, and it
gives precise control over which version of the data to query, but it may be more tedious on an ongoing basis as 
compared to the Hybrid Store (see below).

## Hybrid Store

The Hybrid Store Design Pattern has been described both in [blog](https://philosopherping.com/hybrid-store-design-pattern)
and in video ([1](https://www.youtube.com/watch?v=hc0pgvnr3fQ), [2](https://www.youtube.com/watch?v=mM-6GysXii4)) 
formats.

Venice supports this pattern out of the box, and it is likely the simplest way to merge batch and real-time data, as
Venice handles the whole orchestration on behalf of the user.