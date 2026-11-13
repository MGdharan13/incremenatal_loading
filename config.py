
import os

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", 3306))
DB_NAME = os.environ.get("DB_NAME", "employee_db")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "root")

JDBC_DRIVER = "com.mysql:mysql-connector-j:8.4.0"

JDBC_URL = (
    f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver"
}
