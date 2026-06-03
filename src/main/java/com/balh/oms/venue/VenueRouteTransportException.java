package com.balh.oms.venue;

/**
 * Venue gRPC call failed (network / UNAVAILABLE / etc.). Egress must not advance the Aeron cursor so
 * restart or the next poll retries the fragment.
 */
public final class VenueRouteTransportException extends RuntimeException {

    public VenueRouteTransportException(String message, Throwable cause) {
        super(message, cause);
    }
}
