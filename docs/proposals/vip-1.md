---
layout: default
title: Authentication Service API
parent: Proposals
permalink: /docs/proposals/vip-1.md
---

# Venice Improvement Proposal Template

* **Status**: _Accepted_
* **Author(s)**: _Enrico Olivelli_
* **Pull Request**: [PR 471](https://github.com/linkedin/venice/pull/471)
* **Release**: N/A

## Introduction

Currently, Venice doesn't provide a well-defined extensible way to authenticate clients, but it only supports
TLS based authentication, and it is hard coded.
This VIP proposes a new API to write plugins to authenticate clients.
The first follow-up work will be to implement a plugin to support JWT token based authentication.

## Problem Statement 

Venice's services are based on HTTP/REST APIs, and we need a way to authenticate clients using standard
mechanisms like JWT tokens and OAuth2.
It means that all the services must perform authentication (and authorization) checks on each request.

Therefore, we need a way to write plugins to perform authentication checks, this way it will be easy to 
add more and more mechanisms in the future.

Authentication mechanisms vary a lot from each other, and they are not only based on single steps;
we need to support at least only mechanisms that work well on HTTP and that can be performed in a single step, 
like basic HTTP Authentication, that needs only to use an HTTP Header
or TLS based Authentication, that is based on the client certificate exchanged during the TLS handshake.


## Scope

1. **What is in scope?**

   - Define an API to write plugins to perform authentication checks on Venice components (controller, server and router).
   - The API must support single step authentication mechanisms (JWT, OAuth2, TLS client authentication).
   - Support retrofitting the existing TLS mechanism as a plugin (without introducing the implementation).
   - Ensure that the Authentication API is used by all the services and applied to every request.
   - Ensure that the AuthorizerService is able to use the Authentication API to perform authorization checks.

2. **What is out of scope?**

    - Remove legacy DynamicAccessController
    - Implement a JWT plugin to support JWT token based authentication (this will be a follow-up work) 
    - Implement the TLS client certificate plugin
    - Refactor the AuthorizerService APIs (even if some changes will be needed) 
    - Implement other authentication mechanisms (Kerberos, SAML, etc.)
    - Modify the Admin Tools to support authentication (this will be a follow-up work)
    - Deal with authentication against the PubSub broker (Kafka, Pulsar, etc.)

## Project Justification

This VIP is required in order to allow Venice user to use standard authentication mechanisms like JWT and OAuth2/OpenID Connect. 

For instance JWT, OAuth2 and OpenId connect are widely used by Apache Pulsar users, and introducing these features will
help the adoption of Venice in the Pulsar community.

## Functional specification

The core of the VIP is the AuthenticationService API, that is used by all the services to perform authentication checks.

The AuthenticationService API is a Java interface that mandates the contract for the authentication plugins.

The goal of the AuthenticationService is to map an HTTP request to a user identity, a **Principal**, that can be used by the AuthorizerService.

As the AuthenticationService must work on all the VeniceComponents it won't have any hard dependency on the HTTP layer,
as in Venice we are using multiple technologies, depending on the Component.

```java
public interface AuthenticationService extends Closeable {

   /**
    * Maps an HTTP request to a Principal.
    * Any unchecked exception thrown by this method will be logged and the request will be rejected.
    * @param requestAccessor access the HTTP Request fields
    * @return the Principal or null if the request is not authenticated
    */
   default Principal getPrincipalFromHttpRequest(HttpRequestAccessor requestAccessor);

   /**
    * Generic Wrapper over an HTTP Request.
    */
   interface HttpRequestAccessor {
       String getHeader(String headerName);
      X509Certificate getCertificate();
   }

   /**
    * Lifecycle method, called when the Venice component is initialized.
    * @param veniceProperties the configuration of the component being initialized
    * @throws Exception
    */
   default void initialise(VeniceProperties veniceProperties) throws Exception;

   /**
    * Lifecycle method, called when the Venice component is closed.
    */
   default void close();

}
```

You configure the classname of the AuthenticationService with an entry `authentication.service.class`.
The AuthenticationService is initialised by passing the VeniceProperties read from the configuration file.  

## Proposed Design

Most the work is about introducing the new API and refactoring the existing code to use it.
The legacy Authentication mechanism will initially be left untouched, but when you configure
the new AuthenticationService, the legacy mechanism will be disabled.

AuthenticationService and AuthorizerService will kick-in in spite of the legacy DynamicAccessController.
And then the AuthenticationService the ACL checks will be performed by the AuthorizerService,
and not by the DynamicAccessController.

In Venice clusters in which you configure AuthenticationService and AuthorizerService it is not expected
that all the current admin tools work, especially the ones that are based on the legacy DynamicAccessController (ACLs...).
It is out of the scope of this VIP to modify the admin tools.

Multi-region support is not taken into account because we are only introducing a new API about Authentication,
it depends on every specific mechanism how to implement intra and inter region authentication.

New Authentication mechanism may have an impact on performance depending on the technology user and the implementation.
It is out of the scope of this VIP to enter the details of the performance impact.
It is possible that in the future in order to support some authentication mechanisms we will need to introduce
an asynchronous API to perform authentication checks.

## Development Milestones

The implementation for this VIP introduces:
- the Java API
- the Controller implementation (loading the plugin and calling the API)
- the Router implementation (loading the plugin and calling the API)
- the Server implementation (loading the plugin and calling the API)
- some dummy plugins to test the API

## Test Plan

The implementation will be tested with unit tests and integration tests, main topics:
- AuthenticationService plugin lifecycle (boostrap, initialization, close)
- Verifying that the plugin is invoked by the Controller, Router and Server

## References 

- [AuthenticationProvider API in Apache Pulsar](https://github.com/apache/pulsar/blob/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication/AuthenticationProvider.java)
- [Authentication Service Docs in Apache Pulsar](https://pulsar.apache.org/docs/3.0.x/security-authorization/)
- [OAuth2](https://oauth.net/2/)
- [JWT](https://jwt.io/introduction)









