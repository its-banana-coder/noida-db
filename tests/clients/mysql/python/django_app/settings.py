import os

import pymysql

pymysql.version_info = (2, 2, 1, "final", 0)  # Django checks the mysqlclient version
pymysql.install_as_MySQLdb()
SECRET_KEY = "test"
INSTALLED_APPS = ["django.contrib.contenttypes", "django.contrib.auth", "shop"]
DATABASES = {"default": {"ENGINE": "django.db.backends.mysql", "NAME": "djdb", "USER": "root",
                         "HOST": "127.0.0.1", "PORT": os.environ.get("NOIDA_MYSQL_PORT", "3306")}}
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
