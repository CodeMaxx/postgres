#!/bin/bash

cd install

rm -r data_old
mv data data_old

bin/initdb -D data

sed -i s/\#port/port/ data/postgresql.conf;

bin/pg_ctl -D data -l logfile start;

sleep 2;

bin/createdb -p 5432 test;

bin/psql -h localhost -p 5432 -d test -f start.sql;

bin/pg_ctl -D data stop;
