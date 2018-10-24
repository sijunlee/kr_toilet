#!/bin/bash

usage() {
    echo "Usage: $0 [-d <database name>] [-u <database username>] [-t <tmp directory>]" 1>&2; exit 1;
    }

db_user="postgres"
db_name="kr_geo"

my_dir=$(dirname $(readlink -f $0))

set -e

echo "db_user - ${db_user}"
echo "db_name - ${db_name}"


psql --single-transaction -e -U ${db_user} -d ${db_name} << EOT
DROP TABLE IF EXISTS toilet;

CREATE TABLE IF NOT EXISTS toilet (
		category character varying(200),
		name character varying(200),
		streetAddress character varying(200),
		originalAddress character varying(200),
		phoneNumber character varying(200),
		operationTime character varying(200),
		latitude            float,
        longitude           float,
		moddate             date
	);
\copy toilet ( category, name, streetAddress, originalAddress, phoneNumber, operationTime, latitude, longitude, moddate) from './toilet_20180928_filtering.csv' with delimiter ',' CSV HEADER;
SELECT AddGeometryColumn ('public','toilet','geometry',4326,'POINT',2);
UPDATE toilet SET geometry = ST_PointFromText('POINT(' || longitude || ' ' || latitude || ')', 4326);
CREATE INDEX idx_toilet_geometry ON toilet USING gist(geometry);
ALTER TABLE toilet ADD COLUMN distance float;
EOT

