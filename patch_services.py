import re

with open("src/services.rs", "r") as f:
    content = f.read()

content = re.sub(
    r'match name \{',
    r'match name {\n        #[cfg(feature = "mysql")]\n        "mysql" => Some(crate::mysql::server::spawn(addr)),',
    content
)

with open("src/services.rs", "w") as f:
    f.write(content)
