# import
sqoop import --connect jdbc:postgresql://localhost:5432/postgres \
--username ub --password monster \
--table 'book_reviews' -m 1 \
--class-name BookReviews

# export
sqoop export --connect jdbc:postgresql://localhost:5432/postgres \
--username ub --password monster \
--table 'book_reviews' \
--export-dir /user/ub/book_reviews \
--input-fields-terminated-by '\u0001'


