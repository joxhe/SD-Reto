from flask import Flask, request, render_template, redirect, url_for, session, flash
import mysql.connector
import os
import time

app = Flask(__name__)
app.secret_key = "clave_super_secreta"

def get_db_connection():
    max_attempts = 15
    for attempt in range(max_attempts):
        try:
            print(f"🔄 Intento de conexión {attempt + 1}/{max_attempts}...")
            connection = mysql.connector.connect(
                host='db',
                user='root', 
                password='cecar',
                database='reto_db',
                port=3306,
                connect_timeout=20,
                autocommit=True
            )
            print("✅ ¡Conexión MySQL exitosa!")
            return connection
        except mysql.connector.Error as e:
            print(f"❌ Error MySQL {attempt + 1}: {e}")
            if attempt < max_attempts - 1:
                print("⏳ Esperando 3 segundos antes del siguiente intento...")
                time.sleep(3)
            else:
                print("💥 No se pudo conectar a MySQL después de todos los intentos")
                raise

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        nombre = request.form["nombre"]
        email = request.form["email"]
        password = request.form["password"]

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO users (nombre, email, password) VALUES (%s, %s, %s)",
                           (nombre, email, password))
            conn.commit()
            cursor.close()
            conn.close()
            flash("Usuario registrado con éxito", "success")
            return redirect(url_for("login"))
        except Exception as err:
            flash(f"Error: {err}", "danger")

    return render_template("register.html")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form["email"]
        password = request.form["password"]

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT id, nombre FROM users WHERE email=%s AND password=%s", (email, password))
            user = cursor.fetchone()
            cursor.close()
            conn.close()

            if user:
                session["user_id"] = user[0]
                session["nombre"] = user[1]
                flash("Login exitoso", "success")
                return redirect(url_for("files"))
            else:
                flash("Credenciales incorrectas", "danger")
        except Exception as err:
            flash(f"Error: {err}", "danger")

    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada", "info")
    return redirect(url_for("home"))

@app.route("/files")
def files():
    if "user_id" not in session:
        flash("Debes iniciar sesión", "warning")
        return redirect(url_for("login"))

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT f.id, f.filename, u.nombre, f.upload_date 
            FROM files f 
            JOIN users u ON f.uploader_id = u.id 
            ORDER BY f.upload_date DESC
        """)
        files_list = cursor.fetchall()
        cursor.close()
        conn.close()
    except Exception as err:
        flash(f"Error: {err}", "danger")
        files_list = []

    return render_template("files.html", files=files_list)

@app.route("/upload", methods=["POST"])
def upload():
    if "user_id" not in session:
        flash("Debes iniciar sesión", "warning")
        return redirect(url_for("login"))

    file = request.files["file"]
    if file and file.filename:
        filename = file.filename
        uploads_dir = "/app/uploads"
        os.makedirs(uploads_dir, exist_ok=True)
        
        filepath = os.path.join(uploads_dir, filename)
        file.save(filepath)

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO files (filename, uploader_id) VALUES (%s, %s)",
                           (filename, session["user_id"]))
            conn.commit()
            cursor.close()
            conn.close()
            flash("Archivo subido con éxito", "success")
        except Exception as err:
            flash(f"Error: {err}", "danger")
    else:
        flash("No se seleccionó archivo", "warning")

    return redirect(url_for("files"))

@app.route("/health")
def health():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return {"status": "healthy", "database": "connected"}, 200
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}, 500

if __name__ == "__main__":
    print("🚀 Iniciando Flask...")
    app.run(debug=True, host='0.0.0.0', port=5000)