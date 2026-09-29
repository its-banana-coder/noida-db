import re

with open("Cargo.toml", "r") as f:
    content = f.read()

tests = """
[[test]]
name = "mysql_client"
required-features = ["mysql"]
"""
content = re.sub(r'\[\[test\]\]\nname = "mysql_diff"', tests + '\n[[test]]\nname = "mysql_diff"', content)

with open("Cargo.toml", "w") as f:
    f.write(content)
