package com.example.searchtoilet.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.vividsolutions.jts.geom.Geometry;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;

import static com.example.searchtoilet.utils.GISHelper.calculateGeometry;
import static com.example.searchtoilet.utils.PostgresUtils.*;
import static jooq.com.example.searchtoilet.tables.Toilet.TOILET;
import static org.jooq.impl.DSL.val;


@JsonTypeName("ToiletInformation")
public class ToiletInfo {

    public static ImmutableList<Field<?>> FIELDS = ImmutableList.<Field<?>>builder()
            .addAll(Iterables.transform(Arrays.asList(TOILET.fields()), input -> input.as(input.getName())))
            .build();

    public String getCategory() {
        return category;
    }

    public String getName() {
        return name;
    }

    public String getStreetaddress() {
        return streetaddress;
    }

    public String getOriginaladdress() {
        return originaladdress;
    }

    public String getPhonenumber() {
        return phonenumber;
    }

    public String getOperationtime() {
        return operationtime;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public Double getDistance() {
        return distance;
    }

    public String getModdate() {
        return moddate;
    }

    private String category;
    private String name;
    private String streetaddress;
    private String originaladdress;
    private String phonenumber;
    private String operationtime;
    private Double latitude;
    private Double longitude;
    private Double distance;
    private String moddate;

    @JsonCreator
    public ToiletInfo(@JsonProperty("category") @Nonnull String category,
                      @JsonProperty("name") @Nonnull  String name,
                      @JsonProperty("streetaddress") @Nullable String streetaddress,
                      @JsonProperty("originaladdress") @Nullable String originaladdress,
                      @JsonProperty("phonenumber") @Nullable String phonenumber,
                      @JsonProperty("operationtime") @Nullable String operationtime,
                      @JsonProperty("latitude") @Nonnull  Double latitude,
                      @JsonProperty("longitude") @Nonnull Double longitude,
                      @JsonProperty("moddate") @Nonnull String moddate,
                      @JsonProperty("distance") @Nonnull Double distance
    ) {
        this.category = category;
        this.name = name;
        this.streetaddress = streetaddress;
        this.originaladdress = originaladdress;
        this.phonenumber = phonenumber;
        this.operationtime = operationtime;
        this.latitude = latitude;
        this.longitude = longitude;
        this.moddate = moddate;
        this.distance = distance;
    }

    public static ToiletInfo from(Record record) {

        ToiletInfo toilet = new ToiletInfo(record.getValue(TOILET.CATEGORY), record.getValue(TOILET.NAME),
                record.getValue(TOILET.STREETADDRESS), record.getValue(TOILET.ORIGINALADDRESS),
                record.getValue(TOILET.PHONENUMBER), record.getValue(TOILET.OPERATIONTIME),
                record.getValue(TOILET.LATITUDE), record.getValue(TOILET.LONGITUDE),
                record.getValue(TOILET.MODDATE).toString(),
                (Double) record.getValue( 11 )
                );

        return toilet;
    }

    @Nonnull
    public static ToiletInfo[] findNearestToilet(DSLContext db, @Nonnull GeoPoint basePoint, Integer max_count)  {

        return db
                .select(
                        ImmutableList.<Field<?>>builder()
                                .addAll(Iterables.transform(Arrays.asList(TOILET.fields()), input -> input.as(input.getName())))
                                .add(calclulateDistance(transform( TOILET.GEOMETRY, 5179 ) ,  transform(val(calculateGeometry(basePoint),Geometry.class), 5179)).as("distance"))
                                .build()
                )
                .from(TOILET)
                .where(isWidthin(TOILET.GEOMETRY, val(calculateGeometry(basePoint), Geometry.class), 1000.0))
                .orderBy(calclulateDistance(transform( TOILET.GEOMETRY, 5179 ) ,  transform(val(calculateGeometry(basePoint),Geometry.class), 5179)))
                .limit(max_count)
                .fetch()
                .stream()
                .map(ToiletInfo::from).toArray(ToiletInfo[]::new);
    }
}
