package com.example.searchtoilet.utils;


import com.example.searchtoilet.models.GeoPoint;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.operation.buffer.BufferOp;
import com.vividsolutions.jts.operation.distance.DistanceOp;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class GISHelper {
    private static final Logger log = LogManager.getLogger(GISHelper.class);

    private static final double BUFFER_DISTANCE = 0.0001;
    private static final int ATTEMPT_COUNT = 5;

    public static final GeometryFactory FACTORY = new GeometryFactory(new PrecisionModel(), 4326);
    public static final double EARTH_RADIUS_KM = 6371;
    public static final double EARTH_RADIUS_M = 3959;
    public static final int TRUNCATE_LONLAT_DECIMAL_PLACES = 2;
    public static final int MAX_GEOMETRY_POINTS = 512;


    @Nonnull
    public static final Polygon buildCircle(@Nonnull Point point, double degreesDistance) {
        return (Polygon) BufferOp.bufferOp(point, degreesDistance);
    }

    @Nullable
    public static final Geometry parseWKT(@Nullable String geometryWKT) {
        if (geometryWKT != null) {
            try {
                return new WKTReader().read(geometryWKT);
            } catch (ParseException e) {
                log.warn("Parse error testing if a GeoPoint is within malformed WK:. " + geometryWKT);
            }
        }

        return null;
    }

    public static final double getDistanceInDegrees(@Nonnull Point point1, @Nonnull Point point2) {
        return DistanceOp.distance(point1, point2);
    }


    private static double toDegrees(double value, double earthRadius) {
        return value * (180 / (earthRadius * Math.PI));
    }

    public static final double degreesToMiles(double degrees) {
        return fromDegrees(degrees, EARTH_RADIUS_M);
    }

    public static final double degreesToKilometers(double degrees) {
        return fromDegrees(degrees, EARTH_RADIUS_KM);
    }

    private static double fromDegrees(double degrees, double earthRadius) {
        return degrees * ((earthRadius * Math.PI) / 180);
    }

    public static boolean within(@Nullable Point point, @Nullable Geometry contain) {
        if (point != null && contain != null && contain.isValid()) {
            return point.within(contain);
        } else {
            return false;
        }
    }

    @Nonnull
    public static Geometry calculateGeometry(@Nonnull GeoPoint geoPoint) {
        return FACTORY.createPoint(new Coordinate(geoPoint.getLongitude(), geoPoint.getLatitude()));
    }


    @Nullable
    public static Geometry simplify(@Nullable Geometry geometry) {
        return simplify(geometry, true, 0.02, 0.200, MAX_GEOMETRY_POINTS);
    }

    @Nullable
    public static Geometry simplify(@Nullable Geometry geometry,
                                    boolean preserveTopology,
                                    double initialTolerance,
                                    double maxTolerance,
                                    int maxPoints) {
        int attempts = 0;
        Geometry simplified = geometry;

        if (geometry == null || geometry.getNumPoints() <= maxPoints) {
            return geometry;
        }

        double tolerance = initialTolerance;
        while (attempts <= ATTEMPT_COUNT && tolerance <= maxTolerance && simplified.getNumPoints() > maxPoints) {
            simplified = preserveTopology ?
                    TopologyPreservingSimplifier.simplify(simplified, tolerance) :
                    DouglasPeuckerSimplifier.simplify(simplified, tolerance);

            if (!simplified.isValid()) {
                simplified = simplified.buffer(BUFFER_DISTANCE);
            }
            tolerance *= 4;
        }

        return simplified.isValid() ? simplified : geometry.convexHull().isValid() ? geometry.convexHull() : null;
    }
}
