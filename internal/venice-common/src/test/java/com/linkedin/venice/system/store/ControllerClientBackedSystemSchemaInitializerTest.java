package com.linkedin.venice.system.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ControllerClientBackedSystemSchemaInitializerTest {
  @DataProvider(name = "discoveredControllerTls")
  public Object[][] discoveredControllerTls() {
    return new Object[][] { { true, true }, { false, true }, { false, false } };
  }

  @Test(dataProvider = "discoveredControllerTls")
  public void testD2DiscoveryPreservesTlsFactoryAndRouting(boolean https, boolean supplySslFactory) throws IOException {
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
    HttpServer server;
    if (https) {
      HttpsServer httpsServer = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
      httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslFactory.getSSLContext()));
      server = httpsServer;
    } else {
      server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    }
    Schema schema = Schema.create(Schema.Type.INT);
    server.createContext("/", exchange -> {
      byte[] body = "{\"schemaStr\":\"\\\"int\\\"\",\"schemas\":[{\"id\":1,\"schemaStr\":\"\\\"int\\\"\"}]}"
          .getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, body.length);
      try (OutputStream output = exchange.getResponseBody()) {
        output.write(body);
      }
    });
    server.start();
    try {
      String leaderUrl = (https ? "https" : "http") + "://localhost:" + server.getAddress().getPort();
      RestResponse response = mock(RestResponse.class);
      doReturn(
          ByteString.copyString(
              "{\"cluster\":\"testCluster\",\"url\":\"" + leaderUrl + "\",\"secureUrl\":\"https://localhost:1\"}",
              StandardCharsets.UTF_8)).when(response).getEntity();
      D2Client d2Client = mock(D2Client.class);
      doReturn(CompletableFuture.completedFuture(response)).when(d2Client).restRequest(any());
      try (
          ControllerClientBackedSystemSchemaInitializer initializer = new ControllerClientBackedSystemSchemaInitializer(
              AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
              "testCluster",
              schema,
              null,
              false,
              supplySslFactory ? Optional.of(sslFactory) : Optional.empty(),
              "",
              "controller",
              Optional.of(d2Client),
              "",
              false)) {
        initializer.execute(Collections.singletonMap(1, schema));
      }
    } finally {
      server.stop(0);
    }
  }

  @Test
  public void testCreateSystemStoreAndRegisterSchema() throws IOException {
    Optional<D2Client> d2Client = Optional.of(mock(D2Client.class));
    try (ControllerClientBackedSystemSchemaInitializer initializer = new ControllerClientBackedSystemSchemaInitializer(
        AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
        "testCluster",
        null,
        null,
        true,
        Optional.empty(),
        "",
        "a",
        d2Client,
        "",
        false)) {
      initializer.execute();
    } catch (VeniceException e) {
      Assert.fail("Exception should be thrown when neither controller url nor d2 config is provided");
    }

    try (ControllerClientBackedSystemSchemaInitializer initializer = new ControllerClientBackedSystemSchemaInitializer(
        AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
        "testCluster",
        null,
        null,
        true,
        Optional.empty(),
        "",
        "a",
        Optional.empty(),
        "",
        false)) {
      initializer.execute();
      Assert.fail("Exception should be thrown when neither controller url nor d2 config is provided");
    } catch (VeniceException e) {
      // expected
    }

    try (ControllerClientBackedSystemSchemaInitializer initializer = new ControllerClientBackedSystemSchemaInitializer(
        AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
        "testCluster",
        null,
        null,
        true,
        Optional.empty(),
        "",
        "",
        d2Client,
        "",
        false)) {
      initializer.execute();
      Assert.fail("Exception should be thrown when neither controller url nor d2 config is provided");
    } catch (VeniceException e) {
      // expected
    }

    ControllerClient controllerClient = mock(ControllerClient.class);
    try (ControllerClientBackedSystemSchemaInitializer initializer = new ControllerClientBackedSystemSchemaInitializer(
        AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
        "testCluster",
        null,
        null,
        true,
        Optional.empty(),
        "",
        "d2Service",
        d2Client,
        "d2ZkHost",
        false)) {
      doReturn("leaderControllerUrl").when(controllerClient).getLeaderControllerUrl();
      D2ServiceDiscoveryResponse discoveryResponse = mock(D2ServiceDiscoveryResponse.class);
      doReturn(true).when(discoveryResponse).isError();
      doReturn(ErrorType.STORE_NOT_FOUND).when(discoveryResponse).getErrorType();
      doReturn(discoveryResponse).when(controllerClient).discoverCluster(any());
      StoreResponse storeResponse = mock(StoreResponse.class);
      doReturn(true).when(storeResponse).isError();
      doReturn(ErrorType.STORE_NOT_FOUND).when(storeResponse).getErrorType();
      doReturn(storeResponse).when(controllerClient).getStore(any());
      NewStoreResponse newStoreResponse = mock(NewStoreResponse.class);
      doReturn(newStoreResponse).when(controllerClient).createNewSystemStore(any(), any(), any(), any());
      MultiSchemaResponse multiSchemaResponse = mock(MultiSchemaResponse.class);
      MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[2];
      Schema valueSchema = Utils.getSchemaFromResource("avro/StoreMetaValue/v1/StoreMetaValue.avsc");
      schemas[0] = new MultiSchemaResponse.Schema();
      schemas[0].setId(1);
      schemas[0].setSchemaStr(valueSchema.toString());
      schemas[1] = new MultiSchemaResponse.Schema();
      schemas[1].setId(1);
      schemas[1].setDerivedSchemaId(1);
      schemas[1]
          .setSchemaStr(WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema).toString());
      doReturn(schemas).when(multiSchemaResponse).getSchemas();
      doReturn(multiSchemaResponse).when(controllerClient).getAllValueAndDerivedSchema(any());
      doReturn(mock(SchemaResponse.class)).when(controllerClient).addValueSchema(any(), any(), anyInt(), any());
      doReturn(mock(SchemaResponse.class)).when(controllerClient).addDerivedSchema(any(), anyInt(), any());
      doCallRealMethod().when(controllerClient).retryableRequest(anyInt(), any(), any());
      doCallRealMethod().when(controllerClient).retryableRequest(anyInt(), any());
      initializer.setControllerClient(controllerClient);
      initializer.execute();
      verify(controllerClient, times(1)).createNewSystemStore(any(), any(), any(), any());
      verify(
          controllerClient,
          times(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion() - 1))
              .addValueSchema(any(), any(), anyInt(), any());
      verify(
          controllerClient,
          times(AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion() - 1))
              .addDerivedSchema(any(), anyInt(), any());
    }
    verify(controllerClient, times(1)).close();
  }

  @Test
  public void testSchemaCompatabilityType() {
    for (AvroProtocolDefinition protocol: AvroProtocolDefinition.values()) {
      try (
          ControllerClientBackedSystemSchemaInitializer initializer = new ControllerClientBackedSystemSchemaInitializer(
              protocol,
              "testCluster",
              null,
              null,
              false,
              Optional.empty(),
              "",
              "",
              Optional.empty(),
              "",
              false)) {
        if (protocol == AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE) {
          Assert.assertEquals(
              initializer.determineSchemaCompatabilityType(),
              DirectionalSchemaCompatibilityType.BACKWARD);
        } else {
          Assert.assertEquals(initializer.determineSchemaCompatabilityType(), DirectionalSchemaCompatibilityType.FULL);
        }
      }
    }
  }
}
