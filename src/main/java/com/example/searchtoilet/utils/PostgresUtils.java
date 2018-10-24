package com.example.searchtoilet.utils;

import com.google.common.collect.Range;
import com.vividsolutions.jts.geom.Geometry;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.CustomCondition;
import org.jooq.impl.CustomField;
import org.jooq.impl.DSL;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.util.postgres.PostgresDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;

import static org.jooq.impl.DSL.*;

/**
 * Created by dlord on 1/9/14.
 */
public final class PostgresUtils {
    public static final Field<Integer> GEOM_SRID = inline(4326);
    public static final Field<String> TERM_CONFIG = inline("english");
    public static final Field<String> SIMPLE_CONFIG = inline("simple");
    public static final Field<Integer> MAX_EDIT_DISTANCE = inline(2);
    public static final Field<Integer> MIN_WORD_LENGTH = inline(4);
    public static final String TERM_QUERY = "termQuery";
    public static final String TERM_QUERY_VALUE = "value";
    public static final String SMOOTH_SHAPE = "smooth_shape";

    private static List<Tuple2<Range<Float>, Double>> SIMILARITY_RANGES = Arrays.asList(
            new Tuple2<Range<Float>, Double>(Range.closedOpen(0.9f, 1.0f), 0.75d),
            new Tuple2<Range<Float>, Double>(Range.closedOpen(0.65f, 0.9f), 0.5d),
            new Tuple2<Range<Float>, Double>(Range.closedOpen(0.5f, 0.65f), 0.33d),
            new Tuple2<Range<Float>, Double>(Range.closedOpen(0.33f, 0.65f), 0.15d)
    );

    @Nonnull
    public static final String removeAccents(@Nonnull String string) {
        final String beforeReplace = Normalizer.normalize(string, Normalizer.Form.NFKD);
        final String replacedString = beforeReplace.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
        return Normalizer.normalize(replacedString, Normalizer.Form.NFKC);
    }

    @Nullable
    public static String processCode(@Nullable String code) {
        if (code == null) {
            return null;
        }

        return removeAccents(code).replaceAll("[\\p{Punct}|\\p{Space}]*", "").toUpperCase();
    }

    @Nonnull
    public static final Field<String> emptyString() {
        return inline("").cast(PostgresDataType.TEXT);
    }

    public static final Condition similarTo(@Nonnull Field<String> field, @Nonnull Field<String> value) {
        return new SimilarTo(field, value);
    }

    public static final Condition textSearch(@Nonnull Field<String> field, @Nonnull Field<?> value) {
        return new TextSearch(field, value);
    }

    public static final Condition isValidAsText(@Nonnull Field<String> field) {
        return function("ST_IsValid", Boolean.class, geomFromText(field)).isTrue();
    }

    public static final Condition isValid(@Nonnull Field<Geometry> field) {
        return function("ST_IsValid", Boolean.class, field).isTrue();
    }

    public static final Condition boundingBoxIntersects(@Nonnull Field<Geometry> field, @Nonnull Field<Geometry> value) {
        return new BoundingBoxIntersects(field, value);
    }

    public static final Condition intersects(@Nonnull Field<Geometry> field, @Nonnull Field<Geometry> value) {
        return function("ST_Intersects", Boolean.class, field, value).isTrue();
    }

    public static final Condition boundingBoxContains(@Nonnull Field<Geometry> field, @Nonnull Field<Geometry> value) {
        return new BoundingBoxContains(field, value);
    }

    public static final Condition contains(@Nonnull Field<Geometry> field, @Nonnull Field<Geometry> value) {
        return function("ST_Contains", Boolean.class, field, value).isTrue();
    }

    public static final Field<Geometry> intersection(@Nonnull Field<Geometry> field1, @Nonnull Field<Geometry> field2) {
        return function("ST_Centroid", Geometry.class, function("ST_Intersection", Geometry.class, field1, field2));
    }

    public static final Field<Geometry> geomFromText(@Nonnull Field<String> field) {
        return function("ST_GeomFromText", Geometry.class, field, GEOM_SRID);
    }

    public static final Field<String> geomAsText(@Nonnull Field<Geometry> field) {
        return function("ST_AsText", String.class, field);
    }

    public static final Field<Geometry> buffer(@Nonnull Field<Geometry> field, double buffer) {
        return function("ST_Buffer", Geometry.class, field, inline(buffer));
    }

    public static final Field<Geometry> transform(@Nonnull Field<Geometry> field, int srid) {
        return function("ST_Transform", Geometry.class, field, inline(srid));
    }

    public static final Field<Double> getLongitude(@Nonnull Field<Geometry> field) {
        return function("ST_X", Double.class, field);
    }

    public static final Field<Double> getLatitude(@Nonnull Field<Geometry> field) {
        return function("ST_Y", Double.class, field);
    }

    public static final Field<Double> calclulateDistance(@Nonnull Field<Geometry> field1, @Nonnull Field<Geometry> field2) {
        return function("ST_Distance", Double.class, field1, field2);
    }

    public static final Condition isWidthin(@Nonnull Field<Geometry> point1, Field<Geometry> point2, double distance ) {
        return function("ST_DWithin", Boolean.class, point1, point2, inline(distance)).isTrue();
    }

    public static final Field<Object> toTsVector(@Nonnull Field<String> field) {
        return toTsVector(field, TERM_CONFIG);
    }

    private static final Field<Object> toTsVector(@Nonnull Field<String> field, @Nonnull Field<String> termConfig) {
        return function("to_tsvector", Object.class, termConfig, field);
    }

    public static final Field<Object> toTsQuery(@Nonnull Field<String> field) {
        return toTsQuery(field, TERM_CONFIG);
    }

    private static final Field<Object> toTsQuery(@Nonnull Field<String> field, @Nonnull Field<String> termConfig) {
        return function("to_tsquery", Object.class, termConfig, field);
    }

    public static final Field<Object> plainToTsQuery(@Nonnull Field<String> field) {
        return function("plainto_tsquery", Object.class, TERM_CONFIG, field);
    }

    public static final Field<Float> similarity(@Nonnull Field<String> field, @Nonnull Field<String> value) {
        return function("similarity", Float.class, value, field);
    }

    public static final Field<Double> similarityCode(@Nonnull Field<String> field, @Nonnull Field<String> value) {
        final Field<Float> similarity = similarity(field, value);

        final CaseConditionStep<Double> retval = decode().when(similarity.equal(inline(1.0f)), inline(1.0));
        for (final Tuple2<Range<Float>, Double> range : SIMILARITY_RANGES) {
            retval.when(
                    similarity.lt(inline(range.v1.upperEndpoint())).and(similarity.greaterOrEqual(inline(range.v1.lowerEndpoint()))),
                    inline(range.v2)
            );
        }

        return retval.otherwise(inline(0.05d));
    }

    public static Table buildTermQuery(Table wordsTable, Field<String> wordsField, Field<String> rawQuery) {

        final Table splitWords =
                unnest(
                        function("string_to_array",
                                String[].class,
                                replace(function("strip", String.class, toTsVector(rawQuery, SIMPLE_CONFIG)).cast(PostgresDataType.TEXT), inline("'"), inline("")),
                                inline(" ")
                        )
                ).asTable("splitWords");
        final Field<String> splitWord = splitWords.field(0);

        final Table<Record1<String>> disjunction =
                select(concat(inline("(")).concat(listAgg(coalesce(wordsField, splitWord), " | ").withinGroupOrderBy(wordsField)).concat(inline(")")))
                        .from(
                                splitWords.leftOuterJoin(wordsTable).on(length(splitWord).gt(MIN_WORD_LENGTH).and(similarTo(wordsField, splitWord)))
                        )
                        .where(wordsField.isNull())
                        .or(wordsField.isNotNull().and(function("levenshtein", Integer.class, wordsField, splitWord).lt(MAX_EDIT_DISTANCE)))
                        .groupBy(splitWord)
                        .asTable("disjunction", "value");

        final Table termQuery =
                select(toTsQuery(listAgg(disjunction.field("value"), " & ").withinGroupOrderBy(disjunction.field("value")), TERM_CONFIG).as(TERM_QUERY_VALUE))
                        .from(disjunction)
                        .asTable(TERM_QUERY);

        return termQuery;
    }

    public static Condition arrayContains(@Nonnull Field<String[]> field1, @Nonnull String value) {
        return arrayContains(field1, inline(new String[]{value.toLowerCase()}));
    }

    public static Condition arrayContains(@Nonnull Field<String[]> field1, @Nonnull Field<String[]> field2) {
        return new ArrayContains(field1, field2);
    }

    private static final class SimilarTo extends CustomCondition implements Condition {
        private static final long serialVersionUID = 5975865502724095222L;

        private final Field<String> field;
        private final Field<String> value;
        public SimilarTo(@Nonnull Field<String> field, @Nonnull Field<String> value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public void toSQL(RenderContext context) {
            context
                    .sql("(")
                    .visit(field)
                    .sql(" % ")
                    .visit(value)
                    .sql(")");
        }

        @Override
        public void bind(BindContext context) throws DataAccessException {
            if (!context.configuration().dialect().equals(SQLDialect.POSTGRES)) {
                throw new IllegalArgumentException("Only POSTGRES dialect is supported.");
            }

            context.visit(field);
            context.visit(value);
        }
    }

    private static final class TextSearch extends CustomCondition implements Condition {
        private static final long serialVersionUID = -4308479121893940122L;

        private final Field<Object> vector;
        private final Field<?> value;

        private TextSearch(Field<String> field, Field<?> value) {
            this.vector = toTsVector(field);
            this.value = value;
        }

        @Override
        public void toSQL(RenderContext context) {
            context.sql("(")
                   .visit(vector)
                   .sql(" @@ ")
                   .visit(value)
                   .sql(")");
        }

        @Override
        public void bind(BindContext context) throws DataAccessException {
            if (!context.configuration().dialect().equals(SQLDialect.POSTGRES)) {
                throw new IllegalArgumentException("Only POSTGRES dialect is supported.");
            }

            context.visit(vector);
            context.visit(value);
        }
    }

    private static final class BoundingBoxIntersects extends CustomCondition implements Condition {
        private static final long serialVersionUID = 8906864131444465007L;

        private final Field<Geometry> field1;
        private final Field<Geometry> field2;

        private BoundingBoxIntersects(Field<Geometry> field1, Field<Geometry> field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public void toSQL(RenderContext context) {
            context.sql("(")
                   .visit(field1)
                   .sql(" && ")
                   .visit(field2)
                   .sql(")");
        }

        @Override
        public void bind(BindContext context) throws DataAccessException {
            if (!context.configuration().dialect().equals(SQLDialect.POSTGRES)) {
                throw new IllegalArgumentException("Only POSTGRES dialect is supported.");
            }

            context.visit(field1);
            context.visit(field2);
        }
    }

    private static final class BoundingBoxContains extends CustomCondition implements Condition {
        private static final long serialVersionUID = -7690254363227083797L;

        private final Field<Geometry> field1;
        private final Field<Geometry> field2;

        private BoundingBoxContains(Field<Geometry> field1, Field<Geometry> field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public void toSQL(RenderContext context) {
            context.sql("(")
                   .visit(field1)
                   .sql(" ~ ")
                   .visit(field2)
                   .sql(")");
        }

        @Override
        public void bind(BindContext context) throws DataAccessException {
            if (!context.configuration().dialect().equals(SQLDialect.POSTGRES)) {
                throw new IllegalArgumentException("Only POSTGRES dialect is supported.");
            }

            context.visit(field1);
            context.visit(field2);
        }
    }

    private static final class BoundingBoxDistance extends CustomField<Double> implements Field<Double> {
        private static final long serialVersionUID = -2152899726544896089L;

        private final Field<Geometry> field1;
        private final Field<Geometry> field2;

        private BoundingBoxDistance(Field<Geometry> field1, Field<Geometry> field2) {
            super("bbDistance", DSL.getDataType(Double.class));
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public void toSQL(RenderContext context) {
            context.sql("(")
                   .visit(field1)
                   .sql(" <#> ")
                   .visit(field2)
                   .sql(")");
        }

        @Override
        public void bind(BindContext context) throws DataAccessException {
            if (!context.configuration().dialect().equals(SQLDialect.POSTGRES)) {
                throw new IllegalArgumentException("Only POSTGRES dialect is supported.");
            }

            context.visit(field1);
            context.visit(field2);
        }
    }

    private static final class ArrayContains extends CustomCondition implements Condition {
        private static final long serialVersionUID = 6731000276417129393L;

        private final Field<String[]> field1;
        private final Field<String[]> field2;

        private ArrayContains(Field<String[]> field1, Field<String[]> field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public void toSQL(RenderContext context) {
            context.sql("(")
                   .visit(field1)
                   .sql(" @> ")
                   .visit(field2)
                   .sql(")");
        }

        @Override
        public void bind(BindContext context) throws DataAccessException {
            if (!context.configuration().dialect().equals(SQLDialect.POSTGRES)) {
                throw new IllegalArgumentException("Only POSTGRES dialect is supported.");
            }

            context.visit(field1);
            context.visit(field2);
        }

    }

}
