import os
import sys
import json
import csv
import glob
import time
import re
import subprocess
import urllib.parse
from email import message_from_bytes
from email.policy import default
from http.server import HTTPServer, BaseHTTPRequestHandler

# Import local configuration if available
try:
    from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
except Exception:
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD = "localhost", 3306, "employee_db", "root", "root"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
REJECTED_DIR = os.path.join(OUTPUT_DIR, "rejected", "invalid_employees")
WEB_DIR = os.path.join(BASE_DIR, "web")

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_python_executable():
    """Detect the best Python interpreter to run main.py."""
    env_py = os.environ.get("PYTHON_EXEC")
    if env_py and os.path.exists(env_py):
        return env_py
    venv_py = os.path.join(BASE_DIR, ".venv", "Scripts", "python.exe")
    if os.path.exists(venv_py):
        return venv_py
    return sys.executable or "python"

class ETLRequestHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        # Cleaner console logs - safe against variable arg count
        try:
            msg = format % args if args else format
        except Exception:
            msg = str(args)
        sys.stdout.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")

    def _send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode('utf-8')
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, file_path, content_type="text/html; charset=utf-8"):
        if not os.path.exists(file_path):
            self.send_error(404, "File not found")
            return
        with open(file_path, 'rb') as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            index_path = os.path.join(WEB_DIR, "index.html")
            self._send_file(index_path, "text/html; charset=utf-8")

        elif path == "/api/status":
            self._send_json({
                "status": "online",
                "python": get_python_executable(),
                "db": {
                    "host": DB_HOST,
                    "port": DB_PORT,
                    "database": DB_NAME,
                    "user": DB_USER
                }
            })

        elif path == "/api/files":
            files = []
            if os.path.exists(INPUT_DIR):
                for entry in sorted(os.listdir(INPUT_DIR)):
                    full_p = os.path.join(INPUT_DIR, entry)
                    if os.path.isfile(full_p):
                        size_bytes = os.path.getsize(full_p)
                        size_str = f"{size_bytes} B" if size_bytes < 1024 else f"{size_bytes/1024:.1f} KB"
                        _, ext = os.path.splitext(entry)
                        files.append({
                            "name": entry,
                            "path": os.path.join("input", entry),
                            "ext": ext.lower(),
                            "size": size_bytes,
                            "size_str": size_str,
                            "modified": time.ctime(os.path.getmtime(full_p))
                        })
            self._send_json({"files": files})

        elif path == "/api/preview":
            file_param = query.get("file", [""])[0]
            if not file_param:
                self._send_json({"columns": [], "rows": [], "error": "No file parameter"})
                return

            full_path = os.path.join(BASE_DIR, file_param)
            if not os.path.exists(full_path):
                self._send_json({"columns": [], "rows": [], "error": "File does not exist"})
                return

            ext = os.path.splitext(full_path)[1].lower()
            try:
                if ext == ".csv":
                    columns, rows = self._parse_csv(full_path)
                elif ext == ".json":
                    columns, rows = self._parse_json(full_path)
                else:
                    columns = ["Notice"]
                    rows = [{"Notice": f"Preview not directly supported in browser for {ext}. Ready for Spark ETL."}]
                self._send_json({"columns": columns, "rows": rows})
            except Exception as e:
                self._send_json({"columns": ["Error"], "rows": [{"Error": str(e)}]})

        elif path == "/api/rejected":
            csv_files = glob.glob(os.path.join(REJECTED_DIR, "part-*.csv"))
            if not csv_files:
                self._send_json({"columns": [], "rows": []})
                return
            latest_csv = max(csv_files, key=os.path.getmtime)
            columns, rows = self._parse_csv(latest_csv)
            self._send_json({"columns": columns, "rows": rows, "source": os.path.basename(latest_csv)})

        elif path == "/api/test-db":
            res = self._test_mysql()
            self._send_json(res)

        elif path == "/api/db-records":
            res = self._query_mysql_records()
            self._send_json(res)

        elif path == "/api/db-stats":
            res = self._query_db_stats()
            self._send_json(res)

        else:
            self.send_error(404, "Endpoint not found")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/run-etl":
            content_length = int(self.headers.get("Content-Length", 0))
            body_raw = self.rfile.read(content_length).decode('utf-8')
            try:
                data = json.loads(body_raw)
            except Exception:
                data = {}

            file_arg = data.get("file", "").strip()
            if not file_arg:
                self._send_json({"success": False, "error": "No input file specified"})
                return

            # Normalize file path
            full_input_path = os.path.abspath(os.path.join(BASE_DIR, file_arg))
            py_exe = get_python_executable()

            cmd = [py_exe, "main.py", "--input", full_input_path]
            start_t = time.time()
            try:
                proc = subprocess.Popen(
                    cmd,
                    cwd=BASE_DIR,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace"
                )
                output, _ = proc.communicate(timeout=300)
                duration = time.time() - start_t
                exit_code = proc.returncode

                # Parse metrics from logs
                metrics = {
                    "original": self._extract_metric(r"Original records:\s*(\d+)", output),
                    "valid": self._extract_metric(r"Valid records:\s*(\d+)", output),
                    "rejected": self._extract_metric(r"Rejected records:\s*(\d+)", output),
                    "deduplicated": self._extract_metric(r"Records after deduplication:\s*(\d+)", output),
                    "affected": self._extract_metric(r"Affected rows:\s*(\d+)", output)
                }

                success = (exit_code == 0) and ("ETL COMPLETED" in output)
                self._send_json({
                    "success": success,
                    "exit_code": exit_code,
                    "duration": duration,
                    "logs": output,
                    "metrics": metrics,
                    "error": None if success else "ETL process failed or returned non-zero exit code"
                })
            except subprocess.TimeoutExpired:
                proc.kill()
                self._send_json({"success": False, "error": "Process timed out after 300s", "logs": "ETL Timed out."})
            except Exception as e:
                self._send_json({"success": False, "error": str(e), "logs": f"Subprocess exception: {str(e)}"})

        elif path == "/api/upload":
            content_type = self.headers.get('Content-Type', '')
            content_length = int(self.headers.get('Content-Length', 0))
            raw_body = self.rfile.read(content_length)

            try:
                msg_bytes = f"Content-Type: {content_type}\r\n\r\n".encode('utf-8') + raw_body
                msg = message_from_bytes(msg_bytes, policy=default)

                saved_filename = None
                for part in msg.iter_parts():
                    filename = part.get_filename()
                    if filename:
                        filename = os.path.basename(filename)
                        save_path = os.path.join(INPUT_DIR, filename)
                        payload = part.get_payload(decode=True)
                        with open(save_path, 'wb') as fout:
                            fout.write(payload)
                        saved_filename = filename
                        break

                if saved_filename:
                    self._send_json({
                        "success": True,
                        "filename": saved_filename,
                        "path": os.path.join("input", saved_filename)
                    })
                else:
                    self._send_json({"success": False, "error": "No file content detected in upload"}, status=400)
            except Exception as e:
                self._send_json({"success": False, "error": f"Upload failed: {str(e)}"}, status=500)

        else:
            self.send_error(404, "Endpoint not found")

    def _extract_metric(self, pattern, text):
        match = re.search(pattern, text)
        return int(match.group(1)) if match else None

    def _parse_csv(self, file_path, limit=100):
        columns = []
        rows = []
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames or []
            for i, row in enumerate(reader):
                if i >= limit:
                    break
                rows.append(row)
        return columns, rows

    def _parse_json(self, file_path, limit=100):
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read().strip()
            data = json.loads(content)

        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            data = [{"value": data}]

        columns = []
        if data:
            keys = set()
            for item in data[:limit]:
                if isinstance(item, dict):
                    keys.update(item.keys())
            columns = sorted(list(keys))

        rows = data[:limit]
        return columns, rows

    def _test_mysql(self):
        try:
            import mysql.connector
            conn = mysql.connector.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connection_timeout=5
            )
            version = conn.get_server_info()
            conn.close()
            return {"connected": True, "version": f"MySQL {version}", "message": "Successfully connected"}
        except ImportError:
            return {"connected": False, "message": "mysql-connector-python is not installed in the active environment"}
        except Exception as e:
            return {"connected": False, "message": str(e)}

    def _query_db_stats(self):
        """Return live DB counts for KPI display."""
        try:
            import mysql.connector
            conn = mysql.connector.connect(
                host=DB_HOST, port=DB_PORT, database=DB_NAME,
                user=DB_USER, password=DB_PASSWORD, connection_timeout=5
            )
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM employees")
            total = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM employee_staging")
            staging = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return {"total_employees": total, "staging_rows": staging, "connected": True}
        except ImportError:
            return {"total_employees": None, "connected": False, "error": "mysql-connector-python not installed"}
        except Exception as e:
            return {"total_employees": None, "connected": False, "error": str(e)}

    def _query_mysql_records(self, limit=100):
        try:
            import mysql.connector
            conn = mysql.connector.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connection_timeout=5
            )
            cursor = conn.cursor(dictionary=True)
            cursor.execute(f"SELECT employee_id, name, department, salary, joining_date, updated_at FROM employees ORDER BY updated_at DESC LIMIT {limit}")
            rows = cursor.fetchall()
            columns = ["employee_id", "name", "department", "salary", "joining_date", "updated_at"]
            cursor.close()
            conn.close()
            return {"columns": columns, "rows": rows}
        except ImportError:
            return {"error": "mysql-connector-python not installed in current Python environment", "columns": [], "rows": []}
        except Exception as e:
            return {"error": f"Database error: {str(e)}", "columns": [], "rows": []}

def run_server(port=8000):
    server_address = ('', port)
    httpd = HTTPServer(server_address, ETLRequestHandler)
    print(f"\n=======================================================")
    print(f"🚀 Incremental ETL Web UI Server running!")
    print(f"🌐 Open in browser: http://localhost:{port}")
    print(f"📁 Serving from: {BASE_DIR}")
    print(f"=======================================================\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.server_close()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    run_server(port)
