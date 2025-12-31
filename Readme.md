CREATEDB db-test.axm
CREATE TABLE users (id BIGINT, name TEXT, age INT)
INSERT INTO users VALUES ((2, 'Alice', 30),(3, 'Bob', 25))
INSERT INTO users VALUES (2, 'Bob', 25)
INSERT INTO users VALUES
            (1, 'Alice',  25),
            (2, 'Bob',  30),
            (3, 'Charlie', 35),
            (4, 'Diana', 28),
            (5, 'Eve',  22)
SELECT * FROM users
SELECT * FROM users WHERE age > 26
EXPLAIN SELECT * FROM users WHERE age > 26
ANALYZE
PING
QUIT
SHUTDOWN
