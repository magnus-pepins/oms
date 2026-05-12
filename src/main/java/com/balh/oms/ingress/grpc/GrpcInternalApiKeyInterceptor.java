package com.balh.oms.ingress.grpc;

import com.balh.oms.config.OmsConfig;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Validates {@code x-oms-internal-key} metadata against {@link OmsConfig.Http#getInternalApiKey()} (same as HTTP ingress).
 */
public final class GrpcInternalApiKeyInterceptor implements ServerInterceptor {

    public static final Metadata.Key<String> INTERNAL_API_KEY =
            Metadata.Key.of("x-oms-internal-key", Metadata.ASCII_STRING_MARSHALLER);

    private final OmsConfig omsConfig;

    public GrpcInternalApiKeyInterceptor(OmsConfig omsConfig) {
        this.omsConfig = omsConfig;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String expected = omsConfig.getHttp().getInternalApiKey();
        if (expected == null || expected.isBlank()) {
            call.close(Status.FAILED_PRECONDITION.withDescription("OMS internal API key is not configured"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        String provided = headers.get(INTERNAL_API_KEY);
        if (provided == null || !expected.equals(provided)) {
            call.close(Status.PERMISSION_DENIED.withDescription("invalid or missing x-oms-internal-key metadata"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
    }
}
