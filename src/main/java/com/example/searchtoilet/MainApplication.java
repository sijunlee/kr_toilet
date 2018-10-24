package com.example.searchtoilet;

import com.example.searchtoilet.health.PostgresHealthcheck;
import com.example.searchtoilet.resources.FindToiletEndpoint;
import com.example.searchtoilet.utils.PostgresConfiguration;
import io.dropwizard.Application;
import io.dropwizard.java8.Java8Bundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class MainApplication extends Application<MainServiceConfig> {

    public static void main(String[] args) throws Exception {
        new MainApplication().run(args);
    }

    @Override
    public String getName() {
        return "dropwizard-example";
    }

    @Override
    public void initialize(Bootstrap<MainServiceConfig> bootstrap) {
        bootstrap.addBundle(new Java8Bundle());
    }

    @Override
    public void run(MainServiceConfig configuration, Environment environment) throws ClassNotFoundException {
        final PostgresConfiguration postgresConfiguration = new PostgresConfiguration(
                configuration.getConnectString(),
                configuration.getDatabase(),
                configuration.getPostgresUser(),
                "",
                configuration.isEnableLogging(),
                configuration.getMaxIdleConnections(),
                configuration.getMaxTotalConnections(),
                configuration.getRowLimit()
        );

        environment.lifecycle().manage(postgresConfiguration);
        environment.jersey().register(new FindToiletEndpoint(postgresConfiguration));
        environment.healthChecks().register("postgres", new PostgresHealthcheck(postgresConfiguration));

        //Register resources
//        environment.jersey().register(
//                new HelloWorldResource(
//                        configuration.getTemplate(),
//                        configuration.getDefaultName()
//                )
//        );
    }
}
