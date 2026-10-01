//! Differential test for a small but deliberately nasty e-commerce schema
//! (customers/products/orders/order_items) against a real PostgreSQL
//! server, following the same convention as `postgres_diff.rs`: every
//! statement runs on both servers and the results must match -- values,
//! column names and column types.
//!
//! The reference server is `NOIDA_POSTGRES_REF=host:port` if set (CI points
//! this at postgres:16), otherwise a local `initdb`/`postgres` pair started in
//! a temporary directory. With neither, the test prints SKIPPED and passes.
//!
//! The 50 queries exercise joins, aggregates, correlated subqueries,
//! `EXISTS`/`NOT EXISTS`, CTEs (including a recursive one over a
//! self-referencing `customers.referred_by` tree), window functions
//! (`ROW_NUMBER`, `RANK`, `LAG`, running sums), `FILTER`, `ILIKE`,
//! `EXTRACT`, `DATE_TRUNC`, `COALESCE`/`NULLIF`, `ALL`, and set operations
//! (`INTERSECT`/`EXCEPT`).
//!
//! Query 9 is intentionally run in two forms: a version that double-counts
//! `shipping_fee` once per line item (a classic aggregation trap -- `SUM`
//! over a join fans out one-to-many, so summing a per-order value inside
//! that same `GROUP BY` repeats it), and a corrected version that sums
//! shipping per order first. Both are still valid differential checks:
//! whatever the broken query's exact semantics are, noida-db must compute
//! the *same* number real Postgres does, trap included -- that's what
//! actually catches a wrong `SUM`/`GROUP BY` implementation, rather than
//! only ever exercising already-correct queries.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, SimpleQueryMessage};

const SCHEMA_AND_SEED: &[&str] = &[
    "CREATE TABLE customers (
        customer_id     INTEGER PRIMARY KEY,
        name            VARCHAR(100) NOT NULL,
        city            VARCHAR(100) NOT NULL,
        signup_date     DATE NOT NULL,
        tier            VARCHAR(20) NOT NULL,
        referred_by     INTEGER NULL REFERENCES customers(customer_id)
    )",
    "CREATE TABLE products (
        product_id      INTEGER PRIMARY KEY,
        name            VARCHAR(150) NOT NULL,
        category        VARCHAR(50) NOT NULL,
        launch_date     DATE NOT NULL,
        unit_price      NUMERIC(12,2) NOT NULL,
        cost_price      NUMERIC(12,2) NOT NULL,
        stock           INTEGER NOT NULL,
        tax_rate        NUMERIC(5,4) NOT NULL
    )",
    "CREATE TABLE orders (
        order_id        INTEGER PRIMARY KEY,
        customer_id     INTEGER NOT NULL REFERENCES customers(customer_id),
        order_date      DATE NOT NULL,
        status          VARCHAR(20) NOT NULL,
        payment_method  VARCHAR(20) NOT NULL,
        shipping_fee    NUMERIC(12,2) NOT NULL,
        coupon_code     VARCHAR(50) NULL
    )",
    "CREATE TABLE order_items (
        item_id         INTEGER PRIMARY KEY,
        order_id        INTEGER NOT NULL REFERENCES orders(order_id),
        product_id      INTEGER NOT NULL REFERENCES products(product_id),
        quantity        INTEGER NOT NULL,
        unit_price      NUMERIC(12,2) NOT NULL,
        discount_pct    NUMERIC(5,4) NOT NULL
    )",
    "INSERT INTO customers (customer_id, name, city, signup_date, tier, referred_by) VALUES
        (1, 'Alice', 'Delhi',     '2025-01-10', 'gold',   NULL),
        (2, 'Bob',   'Mumbai',    '2025-02-15', 'silver', 1),
        (3, 'Carol', 'Bengaluru', '2025-03-01', 'gold',   1),
        (4, 'Dave',  'Pune',      '2025-03-20', 'bronze', 2),
        (5, 'Eve',   'Delhi',     '2025-04-05', 'silver', 3),
        (6, 'Frank', 'Hyderabad', '2025-04-10', 'gold',   3),
        (7, 'Grace', 'Chennai',   '2025-04-10', 'bronze', 5),
        (8, 'Heidi', 'Kolkata',   '2025-04-20', 'silver', 2)",
    "INSERT INTO products (product_id, name, category, launch_date, unit_price, cost_price, stock, tax_rate) VALUES
        (101, 'Laptop Pro',                  'Electronics', '2025-01-01', 120000, 90000, 15, 0.18),
        (102, 'Mechanical Keyboard',         'Electronics', '2025-01-15',   8000,  5000, 50, 0.18),
        (103, 'Monitor 4K',                  'Electronics', '2025-02-01',  35000, 22000, 20, 0.18),
        (104, 'USB-C Hub',                   'Accessories', '2025-02-10',   4500,  2500,100, 0.18),
        (105, 'Office Chair',                'Furniture',   '2025-02-20',  18000, 11000, 30, 0.18),
        (106, 'Standing Desk',               'Furniture',   '2025-03-05',  28000, 18000, 18, 0.18),
        (107, 'Webcam',                      'Accessories', '2025-03-10',   7000,  4200, 40, 0.18),
        (108, 'Noise Cancelling Headphones', 'Audio',       '2025-03-25',  15000,  9000, 25, 0.18),
        (109, 'SSD 2TB',                     'Storage',     '2025-04-01',  14000,  8500, 35, 0.18),
        (110, 'Desk Lamp',                   'Furniture',   '2025-04-10',   3500,  1800, 60, 0.18)",
    "INSERT INTO orders (order_id, customer_id, order_date, status, payment_method, shipping_fee, coupon_code) VALUES
        (1001, 1, '2025-04-01', 'completed', 'upi',  250, 'WELCOME10'),
        (1002, 2, '2025-04-03', 'completed', 'card', 150, NULL),
        (1003, 3, '2025-04-07', 'completed', 'card',   0, 'VIP8'),
        (1004, 1, '2025-04-12', 'shipped',   'card', 250, NULL),
        (1005, 4, '2025-04-15', 'completed', 'cod',  300, 'FURN20'),
        (1006, 5, '2025-04-18', 'cancelled', 'card',   0, 'WELCOME10'),
        (1007, 6, '2025-04-20', 'completed', 'upi',  200, NULL),
        (1008, 7, '2025-04-22', 'completed', 'card', 150, 'NEW10'),
        (1009, 8, '2025-04-25', 'pending',   'upi',    0, NULL),
        (1010, 2, '2025-05-01', 'completed', 'card', 150, 'AUDIO12'),
        (1011, 3, '2025-05-05', 'completed', 'upi',  200, 'VIP10'),
        (1012, 5, '2025-05-10', 'shipped',   'card', 250, NULL),
        (1013, 6, '2025-05-12', 'completed', 'card',   0, 'PRO5'),
        (1014, 4, '2025-05-18', 'completed', 'upi',  200, 'AUDIO7'),
        (1015, 1, '2025-05-25', 'completed', 'card', 100, 'SPRING5')",
    "INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price, discount_pct) VALUES
        (1,  1001, 101, 1, 120000, 0.10),
        (2,  1001, 102, 2,   8000, 0.05),
        (3,  1001, 104, 1,   4500, 0.00),
        (4,  1002, 103, 1,  35000, 0.15),
        (5,  1002, 108, 2,  15000, 0.10),
        (6,  1003, 101, 1, 120000, 0.08),
        (7,  1003, 107, 1,   7000, 0.00),
        (8,  1003, 109, 2,  14000, 0.05),
        (9,  1004, 106, 1,  28000, 0.10),
        (10, 1004, 110, 2,   3500, 0.00),
        (11, 1005, 105, 2,  18000, 0.20),
        (12, 1005, 110, 1,   3500, 0.00),
        (13, 1006, 101, 1, 120000, 0.10),
        (14, 1007, 106, 2,  28000, 0.05),
        (15, 1007, 108, 1,  15000, 0.00),
        (16, 1008, 104, 2,   4500, 0.00),
        (17, 1008, 107, 1,   7000, 0.10),
        (18, 1009, 109, 1,  14000, 0.00),
        (19, 1010, 102, 3,   8000, 0.05),
        (20, 1010, 108, 1,  15000, 0.12),
        (21, 1010, 110, 1,   3500, 0.00),
        (22, 1011, 103, 2,  35000, 0.10),
        (23, 1011, 109, 1,  14000, 0.00),
        (24, 1012, 105, 1,  18000, 0.15),
        (25, 1012, 106, 1,  28000, 0.00),
        (26, 1013, 101, 1, 120000, 0.05),
        (27, 1013, 104, 3,   4500, 0.08),
        (28, 1014, 108, 2,  15000, 0.07),
        (29, 1014, 107, 2,   7000, 0.00),
        (30, 1015, 102, 1,   8000, 0.00),
        (31, 1015, 103, 1,  35000, 0.20),
        (32, 1015, 110, 3,   3500, 0.05)",
];

/// Q01-Q50 from the user's e-commerce differential-test proposal. Numbered
/// comments match that proposal's own numbering so a failure is easy to
/// trace back to it.
const QUERIES: &[&str] = &[
    // Q01 - Basic count.
    "SELECT COUNT(*) AS customer_count FROM customers",
    // Q02 - GROUP BY + COUNT.
    "SELECT tier, COUNT(*) AS customers FROM customers GROUP BY tier ORDER BY tier",
    // Q03 - Aggregate by category.
    "SELECT category, ROUND(AVG(unit_price), 2) AS avg_price, MIN(unit_price) AS min_price, MAX(unit_price) AS max_price
     FROM products GROUP BY category ORDER BY category",
    // Q04 - Inventory valuation.
    "SELECT category, SUM(unit_price * stock) AS inventory_value
     FROM products GROUP BY category ORDER BY inventory_value DESC",
    // Q05 - Order status distribution.
    "SELECT status, COUNT(*) AS count FROM orders GROUP BY status ORDER BY count DESC, status",
    // Q06 - Total valid revenue.
    "WITH valid_orders AS (
        SELECT order_id, shipping_fee FROM orders WHERE status IN ('completed', 'shipped')
    ),
    totals AS (
        SELECT o.order_id,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) AS item_total,
               o.shipping_fee
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, o.shipping_fee
    )
    SELECT ROUND(SUM(item_total + shipping_fee), 2) AS revenue FROM totals",
    // Q07 - Total tax collected.
    "SELECT ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * p.tax_rate), 2) AS tax
     FROM orders o
     JOIN order_items oi ON oi.order_id = o.order_id
     JOIN products p ON p.product_id = oi.product_id
     WHERE o.status IN ('completed', 'shipped')",
    // Q08 - Shipping revenue.
    "SELECT SUM(shipping_fee) AS shipping_revenue FROM orders WHERE status IN ('completed', 'shipped')",
    // Q09 (trap) - deliberately double-counts shipping_fee once per line item.
    // Kept as a negative test: real Postgres is the oracle for whatever this
    // query's actual (wrong-for-the-business, right-for-the-SQL) semantics
    // are, which is exactly the kind of case that catches a broken SUM/
    // GROUP BY fan-out implementation.
    "SELECT c.customer_id, c.name,
            ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + SUM(DISTINCT o.shipping_fee), 2) AS spend
     FROM customers c
     JOIN orders o ON o.customer_id = c.customer_id
     JOIN order_items oi ON oi.order_id = o.order_id
     JOIN products p ON p.product_id = oi.product_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY c.customer_id, c.name
     ORDER BY spend DESC",
    // Q09 (corrected) - Customer lifetime spend.
    "WITH order_totals AS (
        SELECT o.order_id, o.customer_id,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + o.shipping_fee AS total
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, o.customer_id, o.shipping_fee
    )
    SELECT c.customer_id, c.name, ROUND(SUM(ot.total), 2) AS spend
    FROM customers c
    JOIN order_totals ot ON ot.customer_id = c.customer_id
    GROUP BY c.customer_id, c.name
    ORDER BY spend DESC",
    // Q10 - Average order value.
    "WITH order_totals AS (
        SELECT o.order_id,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + o.shipping_fee AS total
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, o.shipping_fee
    )
    SELECT ROUND(AVG(total), 2) AS average_order_value FROM order_totals",
    // Q11 - Maximum order.
    "WITH order_totals AS (
        SELECT o.order_id, c.name,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + o.shipping_fee AS total
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, c.name, o.shipping_fee
    )
    SELECT order_id, name, ROUND(total,2) FROM order_totals ORDER BY total DESC LIMIT 1",
    // Q12 - Orders above 100K.
    "WITH order_totals AS (
        SELECT o.order_id, c.name,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + o.shipping_fee AS total
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, c.name, o.shipping_fee
    )
    SELECT order_id, name, ROUND(total,2) AS total FROM order_totals WHERE total > 100000 ORDER BY total DESC",
    // Q13 - Units sold per product.
    "SELECT p.product_id, p.name, SUM(oi.quantity) AS units
     FROM products p
     JOIN order_items oi ON oi.product_id = p.product_id
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY p.product_id, p.name
     ORDER BY units DESC, p.product_id",
    // Q14 - Product revenue.
    "SELECT p.product_id, p.name,
            ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)), 2) AS revenue
     FROM products p
     JOIN order_items oi ON oi.product_id = p.product_id
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY p.product_id, p.name
     ORDER BY revenue DESC",
    // Q15 - Revenue by category.
    "SELECT p.category,
            ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)), 2) AS revenue
     FROM products p
     JOIN order_items oi ON oi.product_id = p.product_id
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY p.category
     ORDER BY revenue DESC",
    // Q16 - Product margin.
    "SELECT p.product_id, p.name,
            ROUND(SUM(oi.quantity * (oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate) - p.cost_price)), 2) AS margin
     FROM products p
     JOIN order_items oi ON oi.product_id = p.product_id
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY p.product_id, p.name
     ORDER BY margin DESC",
    // Q17 - Products above category average (correlated subquery).
    "SELECT product_id, name, category, unit_price
     FROM products p
     WHERE unit_price > (SELECT AVG(p2.unit_price) FROM products p2 WHERE p2.category = p.category)
     ORDER BY product_id",
    // Q18 - Customers above average customer spend.
    "WITH customer_spend AS (
        SELECT c.customer_id, c.name,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + SUM(o.shipping_fee) AS spend
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY c.customer_id, c.name
    ),
    avg_spend AS (SELECT AVG(spend) AS value FROM customer_spend)
    SELECT customer_id, name, ROUND(spend,2)
    FROM customer_spend
    WHERE spend > (SELECT value FROM avg_spend)
    ORDER BY spend DESC",
    // Q19 - Customers with >=2 valid orders.
    "SELECT c.customer_id, c.name, COUNT(o.order_id) AS order_count
     FROM customers c
     JOIN orders o ON o.customer_id = c.customer_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY c.customer_id, c.name
     HAVING COUNT(o.order_id) >= 2
     ORDER BY c.customer_id",
    // Q20 - Orders containing >2 line items.
    "SELECT o.order_id, COUNT(*) AS line_items
     FROM orders o
     JOIN order_items oi ON oi.order_id = o.order_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY o.order_id
     HAVING COUNT(*) > 2
     ORDER BY o.order_id",
    // Q21 - Orders containing 3+ different categories.
    "SELECT o.order_id, COUNT(DISTINCT p.category) AS categories
     FROM orders o
     JOIN order_items oi ON oi.order_id = o.order_id
     JOIN products p ON p.product_id = oi.product_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY o.order_id
     HAVING COUNT(DISTINCT p.category) >= 3
     ORDER BY o.order_id",
    // Q22 - Average discount by category.
    "SELECT p.category, ROUND(AVG(oi.discount_pct) * 100, 2) AS avg_discount_pct
     FROM products p
     JOIN order_items oi ON oi.product_id = p.product_id
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY p.category
     ORDER BY avg_discount_pct DESC",
    // Q23 - Highest-volume product.
    "SELECT p.name, SUM(oi.quantity) AS units
     FROM products p
     JOIN order_items oi ON oi.product_id = p.product_id
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY p.product_id, p.name
     ORDER BY units DESC LIMIT 1",
    // Q24 - Highest-revenue product.
    "SELECT p.name,
            ROUND(SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)),2) AS revenue
     FROM products p
     JOIN order_items oi ON oi.product_id = p.product_id
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY p.product_id, p.name
     ORDER BY revenue DESC LIMIT 1",
    // Q25 - EXISTS: customers who bought Electronics.
    "SELECT c.customer_id, c.name
     FROM customers c
     WHERE EXISTS (
        SELECT 1 FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.customer_id = c.customer_id
          AND o.status IN ('completed', 'shipped')
          AND p.category = 'Electronics'
     )
     ORDER BY c.customer_id",
    // Q26 - NOT EXISTS: customers with no pending order.
    "SELECT c.customer_id, c.name
     FROM customers c
     WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id AND o.status = 'pending')
     ORDER BY c.customer_id",
    // Q27 - Orders above that customer's own average (window function).
    "WITH order_totals AS (
        SELECT o.order_id, o.customer_id, o.order_date,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + o.shipping_fee AS total
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, o.customer_id, o.order_date, o.shipping_fee
    ),
    with_avg AS (
        SELECT *, AVG(total) OVER (PARTITION BY customer_id) AS customer_avg FROM order_totals
    )
    SELECT order_id, customer_id, ROUND(total,2), ROUND(customer_avg,2)
    FROM with_avg
    WHERE total > customer_avg
    ORDER BY order_id",
    // Q28 - Customers whose max order > 100K.
    "WITH order_totals AS (
        SELECT o.order_id, o.customer_id,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + o.shipping_fee AS total
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, o.customer_id, o.shipping_fee
    )
    SELECT c.customer_id, c.name, ROUND(MAX(ot.total),2) AS max_order
    FROM customers c
    JOIN order_totals ot ON ot.customer_id = c.customer_id
    GROUP BY c.customer_id, c.name
    HAVING MAX(ot.total) > 100000
    ORDER BY max_order DESC",
    // Q29 - Days between signup and first purchase.
    "WITH first_orders AS (
        SELECT customer_id, MIN(order_date) AS first_order_date
        FROM orders WHERE status IN ('completed', 'shipped')
        GROUP BY customer_id
    )
    SELECT c.customer_id, c.name, c.signup_date, f.first_order_date,
           f.first_order_date - c.signup_date AS days_to_first_order
    FROM customers c
    JOIN first_orders f ON f.customer_id = c.customer_id
    ORDER BY c.customer_id",
    // Q30 - Monthly revenue (DATE_TRUNC).
    "WITH order_totals AS (
        SELECT o.order_id, o.order_date,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + o.shipping_fee AS total
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, o.order_date, o.shipping_fee
    )
    SELECT DATE_TRUNC('month', order_date)::date AS month, ROUND(SUM(total),2) AS revenue
    FROM order_totals GROUP BY 1 ORDER BY 1",
    // Q31 - Running monthly revenue (window function).
    "WITH monthly AS (
        SELECT DATE_TRUNC('month', o.order_date)::date AS month,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + SUM(o.shipping_fee) AS revenue
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY 1
    )
    SELECT month, ROUND(revenue,2), ROUND(SUM(revenue) OVER (ORDER BY month), 2) AS running_revenue
    FROM monthly ORDER BY month",
    // Q32 - RANK products inside their category.
    "WITH product_revenue AS (
        SELECT p.category, p.product_id, p.name,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) AS revenue
        FROM products p
        JOIN order_items oi ON oi.product_id = p.product_id
        JOIN orders o ON o.order_id = oi.order_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY p.category, p.product_id, p.name
    )
    SELECT category, product_id, name, ROUND(revenue,2) AS revenue,
           RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS category_rank
    FROM product_revenue
    ORDER BY category, category_rank",
    // Q33 - Customer ranking (RANK).
    "WITH customer_spend AS (
        SELECT c.customer_id, c.name, c.tier,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + SUM(o.shipping_fee) AS spend
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY c.customer_id, c.name, c.tier
    )
    SELECT customer_id, name, ROUND(spend,2), RANK() OVER (ORDER BY spend DESC) AS rank
    FROM customer_spend ORDER BY rank",
    // Q34 - ROW_NUMBER per customer.
    "WITH order_totals AS (
        SELECT o.order_id, o.customer_id, o.order_date,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + o.shipping_fee AS total
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, o.customer_id, o.order_date, o.shipping_fee
    )
    SELECT order_id, customer_id, order_date, ROUND(total,2),
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_number
    FROM order_totals
    ORDER BY customer_id, order_date",
    // Q35 - LAG.
    "WITH order_totals AS (
        SELECT o.order_id, o.customer_id, o.order_date,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + o.shipping_fee AS total
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, o.customer_id, o.order_date, o.shipping_fee
    )
    SELECT order_id, customer_id, ROUND(total,2) AS total,
           ROUND(LAG(total) OVER (PARTITION BY customer_id ORDER BY order_date),2) AS previous_order
    FROM order_totals
    ORDER BY customer_id, order_date",
    // Q36 - CASE classification.
    "WITH order_totals AS (
        SELECT o.order_id,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + o.shipping_fee AS total
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, o.shipping_fee
    )
    SELECT CASE WHEN total >= 100000 THEN 'LARGE' WHEN total >= 50000 THEN 'MEDIUM' ELSE 'SMALL' END AS order_size,
           COUNT(*)
    FROM order_totals GROUP BY 1 ORDER BY 1",
    // Q37 - FILTER aggregate.
    "SELECT c.customer_id, c.name,
            COUNT(*) FILTER (WHERE o.status = 'completed') AS completed,
            COUNT(*) FILTER (WHERE o.status = 'shipped') AS shipped,
            COUNT(*) FILTER (WHERE o.status = 'pending') AS pending,
            COUNT(*) FILTER (WHERE o.status = 'cancelled') AS cancelled
     FROM customers c
     LEFT JOIN orders o ON o.customer_id = c.customer_id
     GROUP BY c.customer_id, c.name
     ORDER BY c.customer_id",
    // Q38 - COALESCE.
    "SELECT order_id, COALESCE(coupon_code, 'NO_COUPON') AS coupon FROM orders ORDER BY order_id",
    // Q39 - ILIKE.
    "SELECT product_id, name FROM products WHERE name ILIKE '%desk%' ORDER BY product_id",
    // Q40 - EXTRACT(DOW).
    "SELECT EXTRACT(DOW FROM order_date) AS day_of_week, COUNT(*) AS orders FROM orders GROUP BY 1 ORDER BY 1",
    // Q41 - Revenue percentage (window function + NULLIF).
    "WITH category_revenue AS (
        SELECT p.category,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) AS revenue
        FROM products p
        JOIN order_items oi ON oi.product_id = p.product_id
        JOIN orders o ON o.order_id = oi.order_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY p.category
    )
    SELECT category, ROUND(revenue / NULLIF(SUM(revenue) OVER (), 0) * 100, 4) AS revenue_pct
    FROM category_revenue ORDER BY revenue DESC",
    // Q42 - Multiple aggregates + NULLIF.
    "SELECT p.category,
            ROUND(AVG(oi.quantity * oi.unit_price), 2) AS avg_gross_line,
            ROUND(SUM(oi.quantity * oi.unit_price * oi.discount_pct) / NULLIF(SUM(oi.quantity * oi.unit_price),0) * 100, 2) AS weighted_discount_pct
     FROM products p
     JOIN order_items oi ON oi.product_id = p.product_id
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.status IN ('completed', 'shipped')
     GROUP BY p.category
     ORDER BY p.category",
    // Q43 - Top 5 products by absolute margin.
    "WITH margins AS (
        SELECT p.product_id, p.name,
               SUM(oi.quantity * (oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate) - p.cost_price)) AS margin
        FROM products p
        JOIN order_items oi ON oi.product_id = p.product_id
        JOIN orders o ON o.order_id = oi.order_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY p.product_id, p.name
    )
    SELECT product_id, name, ROUND(margin,2) FROM margins ORDER BY margin DESC LIMIT 5",
    // Q44 - Recursive CTE over the self-referencing referral tree.
    "WITH RECURSIVE referral_tree AS (
        SELECT customer_id, referred_by, customer_id AS root_id FROM customers
        UNION ALL
        SELECT c.customer_id, c.referred_by, rt.root_id
        FROM customers c
        JOIN referral_tree rt ON c.referred_by = rt.customer_id
    )
    SELECT root_id, COUNT(*) - 1 AS descendant_count
    FROM referral_tree GROUP BY root_id ORDER BY root_id",
    // Q45 - INTERSECT.
    "SELECT customer_id FROM orders WHERE status = 'completed'
     INTERSECT
     SELECT customer_id FROM orders WHERE status = 'shipped'
     ORDER BY customer_id",
    // Q46 - EXCEPT.
    "SELECT customer_id FROM customers
     EXCEPT
     SELECT customer_id FROM orders WHERE status = 'cancelled'
     ORDER BY customer_id",
    // Q47 - NOT EXISTS: customers with no orders at all.
    "SELECT c.customer_id, c.name
     FROM customers c
     WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
     ORDER BY c.customer_id",
    // Q48 - ALL: products pricier than every accessory.
    "SELECT product_id, name, unit_price
     FROM products
     WHERE unit_price > ALL (SELECT unit_price FROM products WHERE category = 'Accessories')
     ORDER BY unit_price",
    // Q49 - Anti-join / correlated NOT EXISTS: top product per category.
    "WITH product_revenue AS (
        SELECT p.category, p.product_id, p.name,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) AS revenue
        FROM products p
        JOIN order_items oi ON oi.product_id = p.product_id
        JOIN orders o ON o.order_id = oi.order_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY p.category, p.product_id, p.name
    )
    SELECT pr.category, pr.product_id, pr.name, ROUND(pr.revenue,2)
    FROM product_revenue pr
    WHERE NOT EXISTS (
        SELECT 1 FROM product_revenue other
        WHERE other.category = pr.category AND other.revenue > pr.revenue
    )
    ORDER BY pr.category",
    // Q50 - The monster: multiple CTEs, joins, aggregates, window functions,
    // FILTER-style correlated logic, category ranking, tier averages.
    "WITH order_totals AS (
        SELECT o.order_id, o.customer_id,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) + o.shipping_fee AS total
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.order_id, o.customer_id, o.shipping_fee
    ),
    customer_spend AS (
        SELECT c.customer_id, c.name, c.tier,
               COALESCE(SUM(ot.total), 0) AS spend,
               COUNT(ot.order_id) AS order_count
        FROM customers c
        LEFT JOIN order_totals ot ON ot.customer_id = c.customer_id
        GROUP BY c.customer_id, c.name, c.tier
    ),
    tier_stats AS (
        SELECT *, AVG(spend) OVER (PARTITION BY tier) AS tier_average FROM customer_spend
    ),
    category_spend AS (
        SELECT o.customer_id, p.category,
               SUM(oi.quantity * oi.unit_price * (1 - oi.discount_pct) * (1 + p.tax_rate)) AS revenue
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE o.status IN ('completed', 'shipped')
        GROUP BY o.customer_id, p.category
    ),
    ranked_categories AS (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY revenue DESC) AS category_rank
        FROM category_spend
    )
    SELECT ts.customer_id, ts.name, ts.tier, ROUND(ts.spend,2) AS spend, ROUND(ts.tier_average,2) AS tier_average,
           rc.category AS top_category, ROUND(rc.revenue,2) AS category_revenue
    FROM tier_stats ts
    JOIN ranked_categories rc ON rc.customer_id = ts.customer_id AND rc.category_rank = 1
    WHERE ts.spend > ts.tier_average
    ORDER BY ts.spend DESC",
];

#[test]
fn ecommerce_differential() {
    let Some(mut reference) = reference() else {
        println!("SKIPPED: no reference Postgres (set NOIDA_POSTGRES_REF or install postgresql)");
        return;
    };
    let addr = noida::postgres::spawn("127.0.0.1:0").expect("start noida-db");
    let noida_url = format!("host=127.0.0.1 port={} user=postgres dbname=postgres", addr.port());
    let mut mine = Client::connect(&noida_url, NoTls).expect("connect to noida-db");

    reset(&mut reference.client);
    reset(&mut mine);

    for stmt in SCHEMA_AND_SEED {
        let want = run(&mut reference.client, stmt);
        let got = run(&mut mine, stmt);
        assert_eq!(want, got, "schema/seed statement diverged: {stmt}");
    }

    let mut compared = 0usize;
    let mut failures = vec![];
    for (i, sql) in QUERIES.iter().enumerate() {
        let want = run(&mut reference.client, sql);
        let got = run(&mut mine, sql);
        compared += 1;
        if want != got {
            failures.push(format!(
                "query {}: {sql}\n  postgres: {want:?}\n  noida-db:    {got:?}",
                i + 1
            ));
            continue;
        }
        if let Some(want_types) = describe(&mut reference.client, sql) {
            let got_types = describe(&mut mine, sql);
            compared += 1;
            if got_types.as_ref() != Some(&want_types) {
                failures.push(format!(
                    "query {} column types: {sql}\n  postgres: {want_types:?}\n  noida-db:    {got_types:?}",
                    i + 1
                ));
            }
        }
    }

    println!("compared {compared} results against PostgreSQL {}", reference.version);
    if !failures.is_empty() {
        panic!("{} differences:\n{}", failures.len(), failures.join("\n"));
    }
}

struct Ref {
    client: Client,
    version: u32,
    _server: Option<Server>,
}

struct Server {
    child: Child,
    dir: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Connects to the reference server named by `NOIDA_POSTGRES_REF`, or starts
/// a local one with initdb. Mirrors `postgres_diff.rs`'s own helper.
fn reference() -> Option<Ref> {
    if let Ok(hostport) = std::env::var("NOIDA_POSTGRES_REF") {
        let (host, port) = hostport.split_once(':').unwrap_or((hostport.as_str(), "5432"));
        let user = std::env::var("NOIDA_POSTGRES_REF_USER").unwrap_or_else(|_| "postgres".into());
        let password =
            std::env::var("NOIDA_POSTGRES_REF_PASSWORD").unwrap_or_else(|_| "postgres".into());
        let url =
            format!("host={host} port={port} user={user} password={password} dbname=postgres");
        let client = wait_for(&url)?;
        let version = server_version(&url)?;
        return Some(Ref { client, version, _server: None });
    }
    let bin =
        ["/usr/lib/postgresql/16/bin", "/usr/lib/postgresql/15/bin", "/usr/lib/postgresql/14/bin"]
            .into_iter()
            .map(Path::new)
            .find(|p| p.join("initdb").exists())?;
    let dir = std::env::temp_dir().join(format!("noida-db-ecommerce-pgref-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let data = dir.join("data");
    std::fs::create_dir_all(&data).ok()?;
    let ok = Command::new(bin.join("initdb"))
        .args([
            "-D",
            data.to_str()?,
            "-A",
            "trust",
            "-U",
            "postgres",
            "--no-sync",
            "--locale=C",
            "-E",
            "UTF8",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .ok()?
        .success();
    if !ok {
        return None;
    }
    let port = free_port();
    let child = Command::new(bin.join("postgres"))
        .args([
            "-D",
            data.to_str()?,
            "-p",
            &port.to_string(),
            "-c",
            "unix_socket_directories=",
            "-c",
            "listen_addresses=127.0.0.1",
            "-c",
            "timezone=UTC",
            "-c",
            "fsync=off",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let server = Server { child, dir };
    let url = format!("host=127.0.0.1 port={port} user=postgres dbname=postgres");
    let client = wait_for(&url)?;
    let version = server_version(&url)?;
    Some(Ref { client, version, _server: Some(server) })
}

fn wait_for(url: &str) -> Option<Client> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match Client::connect(url, NoTls) {
            Ok(c) => return Some(c),
            Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
            Err(_) => return None,
        }
    }
}

fn server_version(url: &str) -> Option<u32> {
    let mut c = Client::connect(url, NoTls).ok()?;
    let row = c.query_one("SHOW server_version_num", &[]).ok()?;
    row.get::<_, &str>(0).parse().ok()
}

#[derive(PartialEq, Debug)]
enum Outcome {
    Rows(Vec<String>, Vec<Vec<Option<String>>>),
    Tag(String),
    Error(String),
}

fn run(client: &mut Client, sql: &str) -> Outcome {
    match client.simple_query(sql) {
        Err(e) => match e.as_db_error() {
            Some(db) => Outcome::Error(db.code().code().to_string()),
            None => Outcome::Error(format!("connection: {e}")),
        },
        Ok(messages) => {
            let mut names = vec![];
            let mut rows = vec![];
            let mut tag = None;
            for m in messages {
                match m {
                    SimpleQueryMessage::Row(r) => {
                        if names.is_empty() {
                            names = r.columns().iter().map(|c| c.name().to_string()).collect();
                        }
                        rows.push((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect());
                    }
                    SimpleQueryMessage::RowDescription(cols) => {
                        names = cols.iter().map(|c| c.name().to_string()).collect();
                    }
                    SimpleQueryMessage::CommandComplete(n) => {
                        tag = Some(n.to_string());
                    }
                    _ => {}
                }
            }
            if names.is_empty() {
                Outcome::Tag(tag.unwrap_or_default())
            } else {
                Outcome::Rows(names, rows)
            }
        }
    }
}

fn describe(client: &mut Client, sql: &str) -> Option<Vec<String>> {
    if !sql.trim_start().to_lowercase().starts_with("select")
        && !sql.trim_start().to_lowercase().starts_with("with")
    {
        return None;
    }
    let stmt = client.prepare(sql).ok()?;
    Some(stmt.columns().iter().map(|c| c.type_().name().to_string()).collect())
}

fn reset(client: &mut Client) {
    let _ = client.simple_query("ROLLBACK");
    let _ = client.simple_query("DROP SCHEMA public CASCADE; CREATE SCHEMA public; RESET ALL");
}
