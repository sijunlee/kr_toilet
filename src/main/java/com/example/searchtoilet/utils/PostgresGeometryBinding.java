package com.example.searchtoilet.utils;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.postgis.PGgeometry;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

/**
 * Created by dlord on 9/23/15.
 */
public class PostgresGeometryBinding implements Binding<Object, Geometry> {
    private static final long serialVersionUID = 7005945025802525453L;

    @Override
    public Converter<Object, Geometry> converter() {
        return new Converter<Object, Geometry>() {
            @Override
            public Geometry from(Object databaseObject) {
                if (databaseObject != null && databaseObject instanceof PGgeometry) {
                    try {
                        final PGgeometry geom = (PGgeometry) databaseObject;
                        final String[] split = PGgeometry.splitSRID(geom.getValue());
                        if (split.length == 2) {
                            return GISHelper.parseWKT(split[1]);
                        }
                    } catch (SQLException e) {
                        return null;
                    }

                }

                return null;
            }

            @Override
            public Object to(Geometry userObject) {
                if (userObject != null) {
                    return new WKTWriter().write(userObject);
                }
                return null;
            }

            @Override
            public Class<Object> fromType() {
                return Object.class;
            }

            @Override
            public Class<Geometry> toType() {
                return Geometry.class;
            }
        };
    }

    @Override
    public void sql(BindingSQLContext<Geometry> ctx) throws SQLException {
        ctx.render()
           .castMode(RenderContext.CastMode.NEVER)
           .visit(DSL.function("ST_GeomFromText", Object.class, DSL.val(ctx.convert(converter()).value(), String.class), PostgresUtils.GEOM_SRID))
           .sql("::geometry");
    }

    @Override
    public void register(BindingRegisterContext<Geometry> ctx) throws SQLException {
        ctx.statement().registerOutParameter(ctx.index(), Types.BINARY);
    }

    @Override
    public void set(BindingSetStatementContext<Geometry> ctx) throws SQLException {
        ctx.statement().setObject(ctx.index(), ctx.convert(converter()).value());
    }

    @Override
    public void set(BindingSetSQLOutputContext<Geometry> ctx) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void get(BindingGetResultSetContext<Geometry> ctx) throws SQLException {
        ctx.convert(converter()).value(ctx.resultSet().getObject(ctx.index()));
    }

    @Override
    public void get(BindingGetStatementContext<Geometry> ctx) throws SQLException {
        ctx.convert(converter()).value(ctx.statement().getObject(ctx.index()));
    }

    @Override
    public void get(BindingGetSQLInputContext<Geometry> ctx) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

}
