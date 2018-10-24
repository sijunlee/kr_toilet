package com.example.searchtoilet.resources;

import com.codahale.metrics.annotation.Metered;
import com.example.searchtoilet.models.ToiletInfo;
import com.example.searchtoilet.models.GeoPoint;
import com.example.searchtoilet.utils.PostgresConfiguration;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import javax.annotation.Nonnull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by dlord on 3/6/17.
 */
@Path("/find/toilet")
@Produces(MediaType.APPLICATION_JSON)
public class FindToiletEndpoint {

    private final PostgresConfiguration config;
    public FindToiletEndpoint(@Nonnull PostgresConfiguration config) {
        this.config = config;
    }

    @GET
    @Metered
    @Path("/{longitude}/{latitude}/{max_count}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response queryByName(@PathParam("longitude") @Nonnull Double longitude,
                                @PathParam("latitude") @Nonnull Double latitude,
                                @PathParam("max_count") @Nonnull Integer max_count) {
        if (longitude == null || latitude == null || max_count == null) {
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        if (max_count == null) {
            max_count = new Integer(10);
        }

        final DSLContext db = DSL.using(config.getConfiguration());
        return Response.ok()
                .entity(ToiletInfo.findNearestToilet(db, new GeoPoint(longitude, latitude), max_count))
                .build();
    }



}
