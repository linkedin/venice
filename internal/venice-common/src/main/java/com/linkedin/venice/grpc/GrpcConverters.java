package com.linkedin.venice.grpc;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;

import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.QueryParams;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.UncompletedPartition;
import com.linkedin.venice.meta.UncompletedReplica;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.GetStoresInClusterGrpcRequest;
import com.linkedin.venice.protocols.GetStoresInClusterGrpcResponse;
import com.linkedin.venice.protocols.QueryJobStatusGrpcRequest;
import com.linkedin.venice.protocols.QueryJobStatusGrpcResponse;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


public class GrpcConverters {
  static {
    ResponseConverterRegistry.registerConverter(
        CreateStoreGrpcResponse.class,
        NewStoreResponse.class,
        GrpcConverters::convertCreateStoreResponse);
    ResponseConverterRegistry.registerConverter(
        GetStoresInClusterGrpcResponse.class,
        MultiStoreInfoResponse.class,
        GrpcConverters::convertGetStoresInClusterResponse);
    ResponseConverterRegistry.registerConverter(
        QueryJobStatusGrpcResponse.class,
        JobStatusQueryResponse.class,
        GrpcConverters::convertQueryJobStatusResponse);

    RequestConverterRegistry.registerConverter(CreateStoreGrpcRequest.class, GrpcConverters::convertCreateStoreRequest);
    RequestConverterRegistry
        .registerConverter(GetStoresInClusterGrpcRequest.class, GrpcConverters::convertGetClusterStoreRequest);
    RequestConverterRegistry
        .registerConverter(QueryJobStatusGrpcRequest.class, GrpcConverters::convertQueryJobStatusRequest);
  }

  public static GrpcControllerRoute mapControllerRouteToGrpcControllerRoute(ControllerRoute route) {
    if (ControllerRoute.NEW_STORE.equals(route)) {
      return GrpcControllerRoute.CREATE_STORE;
    } else if (ControllerRoute.GET_STORES_IN_CLUSTER.equals(route)) {
      return GrpcControllerRoute.GET_STORES_IN_CLUSTER;
    } else if (ControllerRoute.JOB.equals(route)) {
      return GrpcControllerRoute.QUERY_JOB_STATUS;
    } else {
      throw new RuntimeException("Converter for route is not implemented");
    }
  }

  public static <T> HttpToGrpcRequestConverter<T> getRequestConverter(Class<T> requestType) {
    return RequestConverterRegistry.getConverter(requestType);
  }

  public static <S, D> GrpcToHttpResponseConverter<S, D> getResponseConverter(
      Class<S> grpcResponseType,
      Class<D> responseType) {
    return ResponseConverterRegistry.getConverter(grpcResponseType, responseType);
  }

  private static NewStoreResponse convertCreateStoreResponse(CreateStoreGrpcResponse grpcResponse) {
    NewStoreResponse response = new NewStoreResponse();
    response.setOwner(grpcResponse.getOwner());
    return response;
  }

  private static MultiStoreInfoResponse convertGetStoresInClusterResponse(GetStoresInClusterGrpcResponse grpcResponse) {
    ArrayList<StoreInfo> storeInfoList = grpcResponse.getStoresList().stream().map(s -> {
      StoreInfo info = new StoreInfo();
      info.setName(s.getName());
      info.setOwner(s.getOwner());
      info.setCurrentVersion(s.getCurrentVersion());
      info.setColoToCurrentVersions(s.getColoToCurrentVersionMap());
      info.setEnableStoreReads(s.getEnableReads());
      info.setEnableStoreWrites(s.getEnableWrites());
      info.setPartitionCount(s.getPartitionCount());

      return info;

    }).collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

    MultiStoreInfoResponse response = new MultiStoreInfoResponse();
    response.setStoreInfoList(storeInfoList);

    return response;
  }

  private static JobStatusQueryResponse convertQueryJobStatusResponse(QueryJobStatusGrpcResponse grpcResponse) {
    JobStatusQueryResponse response = new JobStatusQueryResponse();
    response.setVersion(grpcResponse.getVersion());
    response.setStatus(grpcResponse.getStatus());
    response.setStatusDetails(grpcResponse.getStatusDetails());
    response.setStatusUpdateTimestamp(grpcResponse.getStatusUpdateTimeStamp());
    response.setUncompletedPartitions(map(grpcResponse.getUncompletedPartitionsList()));
    return response;
  }

  private static List<UncompletedPartition> map(List<QueryJobStatusGrpcResponse.UnCompletedPartition> partitions) {
    List<UncompletedPartition> output = new ArrayList<>();

    partitions.forEach(partition -> {
      UncompletedPartition uncompletedPartition = new UncompletedPartition();
      uncompletedPartition.setPartitionId(partition.getPartition());
      List<UncompletedReplica> mappedUCRs = partition.getUncompletedReplicasList().stream().map(ucr -> {
        UncompletedReplica replica = new UncompletedReplica();
        replica.setStatus(ExecutionStatus.valueOf(ucr.getExecutionStatus()));
        replica.setStatusDetails(ucr.getStatusDetails());
        replica.setCurrentOffset(ucr.getCurrentOffset());
        replica.setInstanceId(ucr.getInstanceId());

        return replica;
      }).collect(Collectors.toList());

      uncompletedPartition.setUncompletedReplicas(mappedUCRs);

      output.add(uncompletedPartition);
    });

    return output;
  }

  // Request converters
  private static CreateStoreGrpcRequest convertCreateStoreRequest(QueryParams params) {
    CreateStoreGrpcRequest.Builder requestBuilder = CreateStoreGrpcRequest.newBuilder()
        .setStoreName(unwrapRequiredOptional(params.getString(NAME)))
        .setKeySchema(unwrapRequiredOptional(params.getString(KEY_SCHEMA)))
        .setValueSchema(unwrapRequiredOptional(params.getString(VALUE_SCHEMA)));

    // set optional fields
    params.getString(OWNER).ifPresent(requestBuilder::setOwner);

    return requestBuilder.build();
  }

  private static GetStoresInClusterGrpcRequest convertGetClusterStoreRequest(QueryParams params) {
    return GetStoresInClusterGrpcRequest.newBuilder()
        .setClusterName(unwrapRequiredOptional(params.getString(CLUSTER)))
        .build();
  }

  private static QueryJobStatusGrpcRequest convertQueryJobStatusRequest(QueryParams params) {
    QueryJobStatusGrpcRequest.Builder requestBuilder = QueryJobStatusGrpcRequest.newBuilder()
        .setName(unwrapRequiredOptional(params.getString(NAME)))
        .setVersion(Integer.parseInt(unwrapRequiredOptional(params.getString(VERSION))));

    params.getString(INCREMENTAL_PUSH_VERSION).ifPresent(requestBuilder::setIncrementalPushVersion);
    params.getString(TARGETED_REGIONS).ifPresent(requestBuilder::setTargetedRegions);

    return requestBuilder.build();
  }

  private static String unwrapRequiredOptional(Optional<String> value) {
    return value.orElseThrow(RuntimeException::new);
  }

  private static class ResponseConverterRegistry {
    private static final Map<Class<?>, Map<Class<?>, GrpcToHttpResponseConverter<?, ?>>> registry = new HashMap<>();

    public static <S, T> void registerConverter(
        Class<S> sourceType,
        Class<T> targetType,
        GrpcToHttpResponseConverter<S, T> converter) {
      registry.computeIfAbsent(sourceType, k -> new HashMap<>()).put(targetType, converter);
    }

    @SuppressWarnings("unchecked")
    public static <S, T> GrpcToHttpResponseConverter<S, T> getConverter(Class<S> sourceType, Class<T> targetType) {
      return (GrpcToHttpResponseConverter<S, T>) registry.getOrDefault(sourceType, new HashMap<>()).get(targetType);
    }
  }

  private static class RequestConverterRegistry {
    private static final Map<Class<?>, HttpToGrpcRequestConverter<?>> registry = new HashMap<>();

    public static <T> void registerConverter(Class<T> requestType, HttpToGrpcRequestConverter<T> converter) {
      registry.computeIfAbsent(requestType, k -> converter);
    }

    @SuppressWarnings("unchecked")
    public static <T> HttpToGrpcRequestConverter<T> getConverter(Class<T> requestType) {
      return (HttpToGrpcRequestConverter<T>) registry.get(requestType);
    }
  }
}
