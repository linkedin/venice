---
layout: default
title: VIP-6 Venice on Kubernetes
parent: Proposals
permalink: /docs/proposals/vip-6
---

# VIP-6: Run Venice on Kubernetes

* **Status**: _Under Discussion_
* **Author(s)**: _Nisarg Thakkar_
* **Pull Request**: [PR 2064](https://github.com/linkedin/venice/pull/2064)
* **Release**: _N/A_

## Introduction

Kubernetes has become the de-facto standard for deploying and managing applications, including complex stateful systems.
As more-and-more organizations adopt Kubernetes for container orchestration and infrastructure management, running
Venice on Kubernetes offers significant benefits in terms of operational automation, deployment flexibility, and
open-source adoption.

This proposal outlines the motivation, challenges, and design considerations for enabling Venice to run natively on
Kubernetes, aiming to improve scalability, resilience, and ease of management for Venice deployments.

## Problem Statement

Venice currently doesn't propose any recommended way to operate Venice. As such, it inherently implies as being run on
traditional virtual machine–based infrastructure or bare metal systems, with BYO operational tooling and deployment
practices. This approach works well for organizations that have mature deployment systems, and dedicated teams to manage
these systems, but it creates significant challenges for those that rely on off-the-shelf products for their compute and
data infrastructure.

Some challenges with the current operational model include:
* Deployment Complexity: Venice operators need to set up and manage physical infrastructure, provisioning,
  configuration, deployment orchestration, making deployments time-consuming and error-prone.
* Operational Overhead: Managing Venice outside of Kubernetes creates operational silos, requiring separate monitoring,
  logging, and maintenance processes.
* Resource Management: Venice does not automatically handle scale up or scale down, and need special tooling or manual
  intervention to manage instance configurations. This limits Venice’s portability across on-prem, hybrid, and
  cloud-native environments.

A vast majority of modern data platforms are being deployed on Kubernetes, which has become the de-facto standard for
container orchestration and infrastructure management. Venice's lack of an off-the-shelf artifact for Kubernetes creates
reluctance in the open-source community to consider Venice as a mature product. It also creates friction for teams that
want to run Venice alongside their Kubernetes-based applications. This misalignment leads to operational challenges,
integration barriers, and scalability limitations, making it difficult for organizations to adopt Venice in
Kubernetes-centric environments.

In addition to community adoption, we will be able to leverage Kubernetes' portability to evolve our certification
testing, and will be able to run cross-version compatibility checks before (or soon after) commits are merged in;
thereby shortening the iterative loop and being able triage regressions faster.

## Scope
Any proposed design for running Venice on Kubernetes must:
1. Preserve Venice’s performance characteristics: High-throughput ingestion and low-latency reads must remain unaffected.
2. Support stateful reliability: Data durability and consistency must be maintained during scaling, failover, and upgrades.
3. Be Kubernetes-native - Deployment, scaling, monitoring, and recovery should leverage Kubernetes primitives
   (StatefulSets, PersistentVolumeClaims, Services, ConfigMaps, etc.) without relying on external orchestration.
4. Integrate with existing infrastructure - The solution must work with standard Kubernetes observability, networking,
   and storage integrations (e.g., Prometheus, CSI drivers, Ingress controllers, etc.).

Improving the performance, scalability, and reliability of Venice itself is out of scope for this proposal. However, if
any design decisions for running Venice on Kubernetes can positively impact these areas, they should be highlighted.

## Project Justification
Adopting Kubernetes will offer the following main benefits to Venice: improved operational reliability, increased
community adoption, ecosystem integration and increased software reliability.

### Improved Operational Reliability
Venice is a distributed, stateful system, and managing such systems manually can be complex and error-prone.
Kubernetes is a de-facto standard for container orchestration, specifically designed to handle the challenges of
distributed applications. By leveraging its core features, we can significantly improve Venice's stability and
operational reliability.

* Automated Self-Healing: Kubernetes continuously monitors the state of all running Pods. If a Venice instance (Pod)
  fails, Kubernetes will automatically restart it or replace it with a new, healthy Pod. This self-healing capability
  minimizes downtime and reduces the need for manual intervention during node failures, a critical feature for a stateful
  database.
* Simplified Scaling and Management: With StatefulSets, Kubernetes provides a stable identity and ordered
  deployment/scaling for each Venice instance. This ensures that even as the cluster scales up or down, each database
  instance maintains its unique network identity and persistent storage.  This capability automates a previously complex
  manual task, making it easier for users to manage their deployments.
* Consistent Environment: Kubernetes provides a consistent, portable runtime environment. This means Venice will
  behave predictably whether it's running on a developer's laptop, a public cloud like AWS or Google Cloud, or an
  on-premise data center. This consistency reduces configuration drift and simplifies troubleshooting.
* Ecosystem Integration: Running Venice on Kubernetes allows it to seamlessly integrate with other cloud-native tools
  and services. For example, it can leverage Kubernetes-native monitoring solutions like Prometheus for metrics
  collection, or use Kubernetes' built-in secrets management for secure configuration. This integration enhances the
  overall functionality and security of Venice deployments using off-the-shelf components.

### Increased Community Adoption
For an open-source project like Venice, a large and active user community is a major goal. Offering first-class
Kubernetes support is a powerful way to remove barriers to entry and significantly expand our user base.

* Meeting User Expectations: Many organizations have already standardized their infrastructure on Kubernetes. By
  providing an easy-to-use, "Kubernetes-native" way to deploy Venice, we meet users where they are. If a user can't
  deploy our database on their existing infrastructure, they'll likely choose a competitor that offers a Kubernetes
  Operator or Helm Chart.
* Lowering the Barrier to Entry: The primary challenge for new users is often not the product itself, but the
  operational complexity of deploying and managing it. We can simplify this with a Helm Chart or a Kubernetes Operator.
  * A Helm Chart simplifies deployment to a single command, making it incredibly easy for users to get started.
  * A Kubernetes Operator goes a step further by automating complex operational tasks like backups, fail-overs, and
    version upgrades, essentially encapsulating the "Day 2" operational knowledge into code. This is a game-changer for
    adoption, as it allows users with less operational experience to run Venice reliably.
* Integration with a Thriving Ecosystem: Kubernetes is at the heart of the cloud-native ecosystem. By providing support,
  Venice instantly becomes interoperable with a wide range of tools for monitoring (Prometheus, Grafana),
  logging (Fluentd), and security. This allows users to seamlessly integrate Venice into their existing cloud-native
  workflows, which is a major selling point and a powerful driver of adoption.
* Community and Legitimacy: Offering deep Kubernetes integration signals to the open-source community that Venice is a
  modern, forward-thinking project. It aligns us with the industry's direction and lends credibility, which is essential
  for attracting new contributors and large-scale enterprise users.
* Community Adoption: Many organizations are adopting Kubernetes as their primary platform for deploying applications.
  By providing a Kubernetes-native deployment option, Venice can attract a broader user base, including those already
  invested in Kubernetes ecosystems. This can lead to increased adoption, community contributions, and a more vibrant
  ecosystem around Venice.

### Increased Software Reliability
Running Venice on Kubernetes can enhance the overall reliability of the software through improved testing, deployment,
and operational practices. Kubernetes offers a portable and consistent environment, which can help run repeatable tests
across different versions and configurations of Venice. This can lead to earlier detection of bugs and performance early
on in the development and release cycle. Following are a few examples of new tests that we will be able to run:
* Cross-Version Compatibility Tests: We can set up automated tests that deploy different versions of Venice and Venice
  clients
* Disaster Recovery Drills: We can simulate node failures, network partitions, and other failure scenarios in a
  controlled manner. This allows us to validate and improve Venice's resilience and recovery mechanisms.

Since Kubernetes ecosystem already has open-source tooling to help us achieve all of this with a small entry barrier,
it makes Kubernetes a very attractive platform for running Venice.

## Functional Specification
There are two main ways to run Venice on Kubernetes: an Easy-Mode and an Advanced-Mode. We'll describe both briefly
here.

The Easy-Mode describes a quick, no-frills setup. It is all about simplicity and a low barrier to entry. We'll leverage
Helm charts and standard Kubernetes resources that are well-understood, well-documented and community maintained.

The Advanced-Mode describes a more complex setup that is more suitable for more complicated deployments suitable for
power-users. It will leverage a Kubernetes Operator to encapsulate the operational knowledge of running Venice on
Kubernetes; and it can extract more value from faster storage mediums like local SSDs and NVMe drives. This mode is more
complex to set up and operate, but it will provide a more robust and production-ready experience.

## Proposed Designs

### The "Easy Mode" - A Helm Chart for Quick Deployment
As mentioned previously, the Easy-Mode describes a quick, no-frills setup. It is all about simplicity and a low barrier
to entry. We'll leverage Helm charts and standard Kubernetes resources that are well-understood, well-documented and
community maintained.

* **Helm Chart**: This will be the user's primary interface. It will package all the necessary Kubernetes resources into
  a single, versioned unit. Initially, this will consist of all Venice components: `venice-router`, `venice-controller`,
  `venice-server`, `zookeeper`, `Kafka`.
  * **User Experience**: Users can get a functional Venice cluster running with a single `helm install` command. The
    `values.yaml` file will allow them to easily customize basic parameters like the number of instances/replicas of each
    component, resource requests, and storage size without editing complex YAML files.
* **`Deployment`**: A `Deployment` is a native Kubernetes resource designed for stateless applications.
  * **Functionality**: It will manage the `venice-router` and `venice-controller` Pods, ensuring that a desired replica
    count is always maintained. When a Pod fails, the `Deployment` will automatically schedule a new Pod to replace it,
    maintaining the cluster's integrity.
  * **Why it's "Easy"**: Users don't need to manage a custom controller and can rely on Kubernetes' built-in,
    battle-tested components for Pod management for stateless workloads.
* **`StatefulSet`**: A `StatefulSet` is a native Kubernetes resource designed for stateful applications.
  * **Functionality**: It will manage the `venice-server`, `kafka` and `zookeeper` Pods, ensuring each instance has a
    stable, unique network identity and persistent storage. When a Pod fails, the `StatefulSet` will automatically restart
    it, maintaining the cluster's integrity.
  * **Why it's "Easy"**: Users don't need to manage a custom controller and can rely on Kubernetes' built-in,
    battle-tested components for Pod management for stateful workloads.
* **Persistent Volume Claim (PVC) and Remote Storage**: The Helm chart will define a `PersistentVolumeClaim` (PVC)
  template, which will dynamically provision remote storage (e.g., from a cloud provider like AWS EBS, Google Cloud
  Persistent Disk, or Azure Disk).
  * **Functionality**: Each `venice-server`, `kafka` and `zookeeper` Pod will get its own dedicated, network-attached
    storage volume. This provides data durability, as the data persists even if a Pod is deleted or moved to a different
    node.
  * **Why it's "Easy"**: This approach works out of the box on most Kubernetes clusters, as it relies on standard
    storage provisioners. It eliminates the need for manual storage configuration or dealing with host-specific resources.

The benefit of this approach is that it provides a simple, reliable way to get started with Venice on Kubernetes. It
leverages well-understood Kubernetes primitives, minimizing the learning curve for users. While it may not offer the
same level of performance as local persistent volumes, it provides a solid foundation that can be built upon in the
future. The downside is that it may not be suitable for very large deployments or those with stringent performance
requirements, but it serves as an excellent entry point for most users. The data durability is also influenced by the
reliability of the remote storage solution used.

DataStax has explored this approach in the past, and have contributed their Helm chart for running Venice on Kubernetes
on their [GitHub repository](https://github.com/datastax/venice-helm-chart). Their experience shows that this approach
is viable and we should evaluate to see if we can leverage their work.

### The "Advanced Mode" - A Custom Kubernetes Operator for Power-Users
The advanced mode is for users with more demanding requirements, particularly around performance and automated "Day 2"
operations. This will be an ambitious project that requires developing a custom controller. `Deployment` offers
most of the resilience for stateless components, however the feature set of `StatefulSet` is limited when it comes to
stateful applications that want to use local persistent storage, and need more complex operational logic to avoid
downtime.

* **Kubernetes Operator**: An Operator is a custom controller that extends the Kubernetes API to manage complex
  applications. It encapsulates domain-specific operational knowledge into code.
  * **Functionality**: Instead of a simple `StatefulSet`, the Operator would watch a custom resource
    (e.g., `VeniceCluster`) and orchestrate the entire lifecycle of the `venice-server` cluster. This allows it to
    perform complex, application-aware actions that a `StatefulSet` cannot.
* **Local SSDs and Storage Management**: A key feature of the advanced mode would be the ability to use high-performance
  local SSDs or NVMe drives.
  * **Challenge**: Local SSDs are "ephemeral" and tied to a specific node. If the host is under maintenance, dies
    ungracefully, or is decommissioned, the data is lost.
  * **Operator's Role**: The Operator would solve this problem by intelligently managing the data. When a Pod needs to
    be moved or a node is decommissioned, the Operator would:
    1. **Schedule a replacement Pod on a different node**: The Operator would first find a suitable node with available
       local SSD capacity and schedule a new `venice-server` Pod there.
    2. **Initiate data migration**: It would then use specific mechanisms to replicate the data available on the
       outgoing node to the replacement node.
    3. **Orchestrate a controlled shutdown**: The Operator would tell the outgoing `venice-server` instance to
       gracefully shut down.
    4. **Ensure consistency**: The Operator would ensure data consistency and integrity throughout the process, only
       marking the migration as complete once the new instance is fully synchronized.
    5. **Release resources**: Finally, it would clean up the old Pod and free up the local SSD resources on the outgoing
       node.
    6. **Monitor and Emit Metrics**: The Operator would continuously monitor the state of this process, continuously
       emitting metrics so that manual operators can track the progress and health of the migration - intervening if
       necessary.
* **Advanced Operational Logic**: Beyond storage, the Operator would automate a range of complex tasks:
  * **Intelligent Scaling**: When a user requests a scale-up or scale-down, the Operator would understand
    `venice-server`'s internal topology and automatically manage the data re-balancing or redistribution.
  * **Increased Fault Tolerance**: The Operator could ensure stricter rack diversity and partition placement rules,
    ensuring that pods get scheduled on instances from various different fault zones, and that partition replicas are also
    distributed across these zones.
  * **Automated Swapping**: The Operator could facilitate zero-downtime node replacements. If a node needs to be
    decommissioned, the Operator would automatically handle the data migration and Pod rescheduling, ensuring continuous
    availability.
  * **Smart Upgrades**: It could perform more sophisticated, planned and unplanned maintenance on the physical nodes
    by being aware of the `venice-server`'s partition availability, ensuring that only a small amount of replicas of each
    partition go offline for maintenance, and also that overall instance availability is not compromised.

To build the Operator, we could either build a custom Operator specifically for Venice, or we could pick one of the
existing general-purpose Operator for stateful systems that are available in the community.

#### Building an Operator - Custom Operator for Venice
A custom-built operator is a controller designed specifically for Venice. It would be written in a language like Go
using a toolset like the Operator SDK or Kubebuilder. This gives us complete control over every aspect of the system's
lifecycle.

##### Pros
* **Complete Customization**: The custom operator would implement Venice-specific operational logic down to the finest
  detail. This includes deeply integrated data migration logic for local SSDs and cluster-aware scaling and rebalancing.
* **Optimized Performance**: The operator can be highly optimized for Venice's architecture, potentially achieving
  better performance for operational tasks than a general-purpose solution.
* **No External Dependency Lock-in**: We would not be dependent on a third-party framework's development cycle or
  feature set. We'll have full ownership of the code and its evolution.
* **Tailored User Experience**: The API for the custom resource (e.g., `VeniceCluster`) can be perfectly tailored to
  Venice users, with simple, declarative fields for features unique to Venice.

##### Cons
* **Significant Development Effort**: Building an operator is a large, complex project that requires deep knowledge of
  both Kubernetes and the Venice internals. This is a substantial time and resource investment.
* **High Maintenance Burden**: We would be responsible for maintaining the entire codebase, including keeping up with
  Kubernetes API changes and security patches.
* **Reinventing the Wheel**: A lot of the core functionality—like metric scraping, backup coordination, and lifecycle
  management is common across all database operators. Building a custom one means we'll have to write this boilerplate
  code ourself.

#### Building an Operator - General-Purpose Stateful Operator
General-purpose operators are frameworks that provide a common control plane for managing various database types.
Instead of building an operator from scratch, we would plug in hooks for Venice operations into their existing
framework.

##### Pros
* **Reduced Development Time**: The core orchestration logic (e.g., Pod management, backup/restore APIs, and
  observability integration) is already built. You only need to provide the Venice-specific "glue" code.
* **Community-Maintained and Battle-Tested**: The underlying framework is maintained by a community or company, which
  means it's often more robust and receives regular updates. You benefit from the collective experience of many users.
* **Unified Control Plane**: Users who run multiple databases can manage them all from a single platform with a
  consistent API and a familiar user experience.
* **Pre-built Features**: These frameworks often come with ready-to-use features like a command-line interface, metrics
  exporters, and Grafana dashboards, which would otherwise need to be built manually.

##### Cons
* **Limited Customization**: We would be constrained by the framework's architecture and API. It might not be possible
  to implement highly specific, Venice-native features or optimize for certain edge cases.
* **Abstraction Overhead**: The framework's generic nature can introduce unnecessary complexity or hide important
  details that would be easier to manage with a custom operator.
* **Vendor Lock-in**: While many are open-source, we would become dependent on a single project's roadmap and design
  choices.

##### Open-Source General-Purpose Operators
There are a few open-source general-purpose operators available currently. As far as I know, none of these are
super-popular as most stateful systems have chosen to build a custom operator. However, Venice is itself a "assembled"
system, and we like to say that we are built on the shoulders of giants. So, considering to leverage existing work
instead of reinventing the wheel is in our culture and spirit.

One such general-purpose operator that I found and briefly evaluated is **KubeBlocks**. It is an open-source,
cloud-native data infrastructure management platform that aims to provide a unified control plane for relational, NoSQL,
and vector databases. It focuses on simplifying day-2 operations, such as scaling, upgrades, and monitoring, and offers
an extensible addon mechanism to integrate new database engines. This could be a good fit for Venice, but we'd have to
evaluate it more thoroughly to ensure it meets our specific needs.

### Recommended Design
This two-tiered approach of offering the the Easy-Mode and the Advanced-Mode provides a clear path for users to adopt
Venice on Kubernetes, starting with a simple, reliable solution and offering a powerful, automated path for
enterprise-grade deployments.

However, I recommend that we start with the Easy-Mode first. This will allow us to move from zero to one quickly, and we
will be able to deliver value to users. We will also hopefully be able to attract interest from the community, and get
more adoption of Venice. Once we have a solid foundation, we can then consider building the more complex Advanced-Mode
as a future project.

Further sections of this proposal only describe the implementation details for the Easy-Mode. The high-level details of
the Advanced-Mode have been mentioned for completeness, but implementation details have been left out-of-scope. That can
be considered as a future work because it involves a lot more complexity; which is needed only by organizations that
have a large deployment footprint of Venice. Our immediate goal of this proposal is to make it easy for users to get
started with Venice on Kubernetes, and the Easy-Mode is sufficient for that.

## [TBD] Development Milestones
_This section described milestones for rather large projects so that we can set up intermediate goals for
better progress management.
For small projects, this may not be necessary._

## [TBD] Test Plan

_Describe in few sentences how the functionality will be tested.
How will we know that the implementation works as expected? How will we know nothing broke?_

1. _What unit tests would be added to cover the critical logic?_
2. _What integration tests would be added to cover major components, if they cannot be tested by unit tests?_
3. _Any tests to cover performance or Security concerns._


## [TBD] References

_List any references here._

## [TBD] Appendix

_Add supporting information here, such as the results of performance studies and back-of-the-envelope calculations.
Organize it however you want but make sure it is comprehensible to your readers._
