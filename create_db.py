import duckdb

# create or open database file
conn = duckdb.connect("data.duckdb")

# create table
conn.execute("""
CREATE TABLE orders (
    order_id INTEGER,
    customer VARCHAR,
    amount INTEGER,
    order_date DATE
)
""")

# insert sample data
conn.execute("""
INSERT INTO orders VALUES
(1, 'Alice', 200, '2024-03-01'),
(2, 'Bob', 300, '2024-03-02'),
(3, 'Alice', 150, '2024-03-03'),
(4, 'Charlie', 500, '2024-03-04')
""")

print("Database and table created successfully!")

conn.close()