-- Create the database
CREATE DATABASE london_trips;

-- connect to the database in psql shell
-- \c london_trips
-- Check if the tables are loaded properly from python
-- \dt

-- Create a table showing the number of nights people stayed vs their reasons for traveling
CREATE TABLE reason_length_il AS (SELECT reason_for_travel,COUNT(travel_type), trip_length FROM international_london GROUP BY reason_for_travel, trip_length ORDER BY reason_for_travel DESC LIMIT 25);
CREATE TABLE reason_length_ll AS (SELECT reason_for_travel,COUNT(travel_type), trip_length FROM local_london GROUP BY reason_for_travel, trip_length ORDER BY reason_for_travel DESC LIMIT 25);

-- What is the most popular mode of travel?
CREATE TABLE pop_travel_il AS (SELECT travel_type, COUNT(reason_for_travel) FROM international_london GROUP BY travel_type ORDER BY travel_type DESC LIMIT 25);
CREATE TABLE pop_travel_ll AS (SELECT travel_type, COUNT(reason_for_travel) FROM local_london GROUP BY travel_type ORDER BY travel_type DESC LIMIT 25);