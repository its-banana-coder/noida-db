import re

with open("Cargo.toml", "r") as f:
    content = f.read()

content = re.sub(
    r'default = \["redis", "postgres"\]',
    'default = ["redis", "postgres", "mysql"]',
    content
)

content = re.sub(
    r'postgres = \["sql", "dep:pgwire", "dep:sqlparser", "dep:bytes", "dep:regex-lite", "dep:rust-stemmers"\]',
    r'postgres = ["sql", "dep:pgwire", "dep:sqlparser", "dep:bytes", "dep:regex-lite", "dep:rust-stemmers"]\nmysql = ["sql", "dep:sqlparser", "dep:bytes"]',
    content
)

tests = """[[test]]
name = "clickhouse_official_client"
required-features = ["clickhouse"]

[[test]]
name = "mysql_diff"
required-features = ["mysql"]
"""
content = re.sub(r'\[\[test\]\]\nname = "clickhouse_official_client"\nrequired-features = \["clickhouse"\]\n?', tests, content)

with open("Cargo.toml", "w") as f:
    f.write(content)
