package com.example.searchtoilet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Created by dlord on 2/2/17.
 */
public class MainServiceConfig extends Configuration {

    private final String connectString;
    private final String database;
    private final boolean enableLogging;
    private final int rowLimit;
    private final String postgresUser;
    private final String postgresPasswordKey;
    private final int maxIdleConnections;
    private final int maxTotalConnections;

    @JsonCreator
    public MainServiceConfig(@JsonProperty("connect_string") @Nonnull String connectString,
                             @JsonProperty("db") @Nonnull String database,
                             @JsonProperty("enable_logging") boolean enableLogging,
                             @JsonProperty("row_limit") int rowLimit,
                             @JsonProperty("postgres_user") @Nonnull String postgresUser,
                             @JsonProperty("postgres_password_key") @Nullable String postgresPasswordKey,
                             @JsonProperty("max_idle_connections") @Nullable Integer maxIdleConnections,
                             @JsonProperty("max_total_connections") @Nullable Integer maxTotalConnections) {
        this.connectString = connectString;
        this.database = database;
        this.enableLogging = enableLogging;
        this.rowLimit = rowLimit;
        this.postgresUser = postgresUser;
        this.postgresPasswordKey = postgresPasswordKey;
        this.maxIdleConnections = maxIdleConnections != null ? maxIdleConnections : 10;
        this.maxTotalConnections = maxTotalConnections != null ? maxTotalConnections : 25;
    }

    @JsonProperty("connect_string")
    public String getConnectString() {
        return connectString;
    }

    @JsonProperty("db")
    public String getDatabase() {
        return database;
    }

    @JsonProperty("enable_logging")
    public boolean isEnableLogging() {
        return enableLogging;
    }

    @JsonProperty("row_limit")
    public int getRowLimit() {
        return rowLimit;
    }

    @JsonProperty("postgres_user")
    public String getPostgresUser() {
        return postgresUser;
    }

    @JsonProperty("postgres_password_key")
    public String getPostgresPasswordKey() {
        return postgresPasswordKey;
    }

    @JsonProperty("max_idle_connections")
    public int getMaxIdleConnections() {
        return maxIdleConnections;
    }

    @JsonProperty("max_total_connections")
    public int getMaxTotalConnections() {
        return maxTotalConnections;
    }
}
