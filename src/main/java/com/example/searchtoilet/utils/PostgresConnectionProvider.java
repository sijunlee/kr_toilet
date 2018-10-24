package com.example.searchtoilet.utils;

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jooq.ConnectionProvider;
import org.jooq.exception.DataAccessException;

import javax.annotation.Nonnull;
import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PostgresConnectionProvider implements ConnectionProvider {
    private static final Logger log = LogManager.getLogger(PostgresConnectionProvider.class);

    private final String databaseName;
    private final String userName;
    private final String password;
    private final double trigramLimit;

    private final GenericObjectPool<Connection> connectionPool;
    private final AtomicInteger counter;

    private List<String> hosts;

    public PostgresConnectionProvider(@Nonnull List<String> hosts,
                                      @Nonnull String databaseName,
                                      @Nonnull String userName,
                                      @Nonnull String password,
                                      int maxIdleConnections,
                                      int maxTotalConnections,
                                      double trigramLimit) {
        this.hosts = hosts.stream().map(host -> "jdbc:postgresql://" + host + "/" + databaseName).collect(Collectors.toList());
        this.databaseName = databaseName;
        this.userName = userName;
        this.password = password;
        this.trigramLimit = trigramLimit;

        this.connectionPool = new GenericObjectPool<>(
                new ConnectionObjectFactory(), buildConfig(maxIdleConnections, maxTotalConnections)
        );
        this.counter = new AtomicInteger();
    }

    @Nonnull
    public synchronized PostgresConnectionProvider setHostsList(@Nonnull List<String> hosts) {
        this.hosts = hosts.stream().map(host -> "jdbc:postgresql://" + host + "/" + databaseName).collect(Collectors.toList());
        return this;
    }

    @Override
    public Connection acquire() throws DataAccessException {
        try {
            return connectionPool.borrowObject();
        } catch (Exception e) {
            throw new DataAccessException("Error when trying to borrow a connection.", e);
        }
    }

    @Override
    public void release(Connection connection) throws DataAccessException {
        connectionPool.returnObject(connection);
    }

    public final void shutdown() {
        connectionPool.close();
    }

    @Nonnull
    private static GenericObjectPoolConfig buildConfig(int maxIdle, int maxTotal) {
        final GenericObjectPoolConfig config = new GenericObjectPoolConfig();

        // GenericObjectPoolConfig values
        config.setMaxIdle(maxIdle);
        config.setMinIdle(1);
        config.setMaxTotal(maxTotal);

        // BaseObjectPoolConfig values
        config.setBlockWhenExhausted(true);
        config.setMaxWaitMillis(650);
        config.setMinEvictableIdleTimeMillis(TimeUnit.MINUTES.toMillis(1L));
        config.setTimeBetweenEvictionRunsMillis(TimeUnit.MINUTES.toMillis(1L));
        config.setTestOnReturn(true);

        return config;
    }

    private final class ConnectionObjectFactory implements PooledObjectFactory<Connection> {

        @Override
        public PooledObject<Connection> makeObject() throws Exception {
            final String host;
            synchronized (PostgresConnectionProvider.this) {
                if (hosts.isEmpty()) {
                    throw new RuntimeException("No hosts defined.");
                }

                host = hosts.get(counter.incrementAndGet() % hosts.size());
            }

            log.trace(String.format(
                    "Attempting: Connection to host '%s' user '%s', trigram limit %s (total %s connections on %s hosts)",
                    host, userName, trigramLimit, counter.get(), hosts.size()
            ));

            Connection connection = DriverManager.getConnection(host, userName, password);
            Statement stmt = connection.createStatement();
            stmt.execute("SELECT set_limit(" + trigramLimit + ")");
            stmt.close();

            log.trace(String.format(
                    "Success: Connection to host '%s' user '%s', trigram limit %s: %s",
                    host, userName, trigramLimit, connection
            ));
            return new DefaultPooledObject<>(connection);
        }

        @Override
        public void destroyObject(@Nonnull PooledObject<Connection> p) throws Exception {
            p.getObject().close();
        }

        @Override
        public boolean validateObject(@Nonnull PooledObject<Connection> p) {
            Statement stmt = null;
            try {
                final Connection connection = p.getObject();
                final boolean valid;
                if (connection != null && hosts.contains(connection.getMetaData().getURL())) {
                    stmt = connection.createStatement();
                    valid = stmt.execute("SELECT 1 = 1");
                } else {
                    valid = false;
                }

                if (valid) {
                    log.trace(String.format("Validated database connection: %s", p.getObject()));
                } else {
                    log.trace(String.format("Failed to validate database connection %s.", p.getObject()));
                }
                return valid;
            } catch (SQLException e) {
                log.trace("Failed to validate database connection.", e);
                return false;
            } finally {
                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void activateObject(PooledObject<Connection> p) throws Exception {
            // TODO: Nothing to do?
        }

        @Override
        public void passivateObject(PooledObject<Connection> p) throws Exception {
            // TODO: Nothing to do?
        }
    }

    /**
     * Command-line helper to test the database connection.
     */
    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser(PostgresConnectionProvider.class.getName())
                                               .description("Test Postgres connections via PostgresConnectionProvider");

        parser.addArgument("--host")
              .required(true)
              .help("host to connect to");
        parser.addArgument("--user")
              .required(true)
              .help("username");
        parser.addArgument("--database")
              .required(true)
              .help("database name");
        parser.addArgument("--password")
              .required(true)
              .help("password");
        Statement statement = null;
        try {
            Namespace res = parser.parseArgs(args);

            String host = res.getString("host");
            String database = res.getString("database");
            String user = res.getString("user");
            String password = res.getString("password");

            log.info(String.format(
                    "Connecting to host '%s' database '%s' username '%s' ...",
                    host, database, user
            ));
            PostgresConnectionProvider connectionProvider = new PostgresConnectionProvider(
                    ImmutableList.of(host),
                    database,
                    user,
                    password,
                    10,
                    50,
                    0.5
            );
            Connection connection = connectionProvider.acquire();
            log.info(String.format("Got connection successfully: %s", connection));

            statement = connection.createStatement();
            boolean success1 = statement.execute("SELECT 1 = 1");
            ResultSet resultSet = statement.getResultSet();
            boolean success2 = resultSet.next();
            boolean success3 = resultSet.getBoolean(1);
            Verify.verify(success1 && success2 && success3, "Statement execute failed: %s %s %s", success1, success2, success3);
            log.info("Used connection successfully");
        } catch (ArgumentParserException e) {
            parser.handleError(e);
        } catch (SQLException e) {
            log.error("SQL exception", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e2) {
                    log.error("SQL exception", e2);

                }
            }
        }
    }
}
