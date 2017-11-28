-- sudo su - postgres
-- psql

CREATE USER ub WITH PASSWORD 'monster';

CREATE DATABASE mydb;

CREATE TABLE book_reviews (
bookID bigint, title varchar, author varchar, rating real,
ratingsCount bigint, reviewsCount bigint, reviewerName varchar,
reviewerRatings int, review varchar);

COPY book_reviews FROM '/home/ub/br.csv' DELIMITER ',' CSV HEADER;

GRANT ALL PRIVILEGES ON DATABASE mydb TO ub;

ALTER USER ub WITH SUPERUSER;

-- sudo su - ub
--psql -d mydb -u ub

CREATE TABLE book_reviews (
bookID bigint, title varchar, author varchar, rating real,
ratingsCount bigint, reviewsCount bigint, reviewerName varchar,
reviewerRatings int, review varchar);

COPY book_reviews FROM '/home/ub/br.csv' DELIMITER ',' CSV HEADER;

