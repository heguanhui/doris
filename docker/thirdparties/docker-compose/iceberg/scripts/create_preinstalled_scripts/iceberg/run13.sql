use demo.test_db;
drop table if exists test_db.partitioned_table;
drop table if exists test_db.unpartitioned_table;
CREATE TABLE IF NOT EXISTS partitioned_table (
    col1 int,
    col2 string,
    col3 int,
    col4 string,
    col5 string
) USING iceberg
PARTITIONED BY (col5);
CREATE TABLE IF NOT EXISTS unpartitioned_table (
    col1 INT,
    col2 VARCHAR(100),
    col3 DECIMAL(10, 2)
) USING iceberg;
INSERT INTO partitioned_table (col1, col2, col3, col4, col5)
VALUES
(1, 'Alice', 25, 'Female', 'New York'),
(2, 'Bob', 30, 'Male', 'Los Angeles'),
(3, 'Charlie', 35, 'Male', 'Chicago'),
(4, 'David', 22, 'Male', 'Houston'),
(5, 'Eve', 28, 'Female', 'Phoenix');
INSERT INTO unpartitioned_table (col1, col2, col3)
VALUES
(1001, 'Product A', 20.00),
(1002, 'Product B', 30.00),
(1003, 'Product C', 40.00),
(1004, 'Product D', 50.00),
(1005, 'Product E', 60.00);
create view view_with_partitioned_table as select * from partitioned_table;
create view view_with_unpartitioned_table as select * from unpartitioned_table;
create view view_with_partitioned_column as select col5 from partitioned_table;
