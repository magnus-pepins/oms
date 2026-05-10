package com.balh.oms.settlement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * JSON body for broker confirms file import (same row shape as {@code POST …/broker-confirms/import-json}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BrokerFixtureFilePayload(List<BrokerFixtureRow> rows) {}
