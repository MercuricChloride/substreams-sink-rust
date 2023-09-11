#!/usr/bin/bash
# Seed a PostgreSQL database with data from a SQL file

#read -p "Database username: " username
#read -p "Database name: " dbname
#read -p "Host: " host

#psql -h $host -U $username -d $dbname -a -f schema.sql
export PGPASSWORD=fart
psql -h localhost -U postgres -d postgres -a -f schema.sql
