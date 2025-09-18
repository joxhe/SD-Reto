from flask import Flask, request, render_template, redirect, url_for, session, flash, send_file, jsonify
import mysql.connector
import os
import time
import requests
import hashlib
import threading
from datetime import datetime
import json
import io

app = Flask(__name__)
app.secret_key = "clave_super_secreta_distribuida"

# --- CONFIGURACIÓN DEL NODO ---
NODE_TYPE = os.getenv("NODE_TYPE", "storage")  # gateway o storage
NODE_ID = os.getenv("NODE_ID", "unknown")
STORAGE_PATH = os.getenv("STORAGE_PATH", "/storage")
STORAGE_NODES = os.getenv("STORAGE_NODES", "").split(",") if os.getenv("STORAGE_NODES") else []

# Crear directorio de almacenamiento
if NODE_TYPE == "storage":
    os.makedirs(STORAGE_PATH, exist_ok=True)
    print(f"📁 Nodo de almacenamiento inicializado: {STORAGE_PATH}")

def get_db_connection():
    """Conexión a la base de datos centralizada"""
    max_attempts = 15
    for attempt in range(max_attempts):
        try:
            print(f"🔄 [{NODE_ID}] Conectando a BD centralizada - intento {attempt + 1}")
            
            config = {
                'host': os.getenv("DB_HOST", "mysql_main"),
                'port': int(os.getenv("DB_PORT", "3306")),
                'user': os.getenv("DB_USER", "root"),
                'password': os.getenv("DB_PASSWORD", "cecar"),
                'database': os.getenv("DB_NAME", "reto_db"),
                'charset': 'utf8mb4',
                'autocommit': True,
                'connect_timeout': 30
            }
            
            connection = mysql.connector.connect(**config)
            print(f"✅ [{NODE_ID}] Conexión BD exitosa!")
            return connection
            
        except Exception as e:
            print(f"❌ [{NODE_ID}] Error BD {attempt + 1}: {e}")
            if attempt < max_attempts - 1:
                time.sleep(3)
            else:
                raise

def calculate_file_hash(file_content):
    """Calcular hash SHA-256 del archivo para distribución"""
    return hashlib.sha256(file_content).hexdigest()

def select_storage_nodes(file_hash):
    """Seleccionar 3 nodos para replicación basado en hash consistente"""
    if not STORAGE_NODES:
        return []
    
    # Hash consistente para distribución determinística
    hash_int = int(file_hash[:8], 16)
    primary_index = hash_int % len(STORAGE_NODES)
    
    # Seleccionar 3 nodos consecutivos (con wrap-around)
    selected = []
    for i in range(min(3, len(STORAGE_NODES))):
        node_index = (primary_index + i) % len(STORAGE_NODES)
        selected.append(STORAGE_NODES[node_index])
    
    return selected

def replicate_to_storage_node(node_url, file_content, filename, file_hash):
    """Replicar archivo a un nodo de almacenamiento específico"""
    try:
        print(f"📤 [{NODE_ID}] Replicando a {node_url}: {filename}")
        
        files = {'file': (filename, io.BytesIO(file_content), 'application/octet-stream')}
        data = {
            'file_hash': file_hash,
            'filename': filename,
            'replica_operation': 'true'
        }
        
        response = requests.post(
            f"http://{node_url}/internal/store", 
            files=files, 
            data=data, 
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"✅ [{NODE_ID}] Replicación exitosa en {node_url}")
            return True
        else:
            print(f"❌ [{NODE_ID}] Error replicación en {node_url}: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ [{NODE_ID}] Error conectando a {node_url}: {e}")
        return False

def retrieve_from_storage_node(node_url, file_hash, filename):
    """Recuperar archivo de un nodo de almacenamiento específico"""
    try:
        response = requests.get(
            f"http://{node_url}/internal/retrieve/{file_hash}_{filename}",
            timeout=15
        )
        
        if response.status_code == 200:
            return response.content
        return None
        
    except Exception as e:
        print(f"❌ [{NODE_ID}] Error recuperando de {node_url}: {e}")
        return None

# =============================================================================
# RUTAS PRINCIPALES (Solo activas en GATEWAY)
# =============================================================================

@app.route("/")
def home():
    if NODE_TYPE != "gateway":
        return jsonify({"error": "Esta ruta solo está disponible en el gateway"}), 404
    return render_template("index.html", node_info={"type": NODE_TYPE, "id": NODE_ID})

@app.route("/register", methods=["GET", "POST"])
def register():
    if NODE_TYPE != "gateway":
        return jsonify({"error": "Registro solo disponible en gateway"}), 404
        
    if request.method == "POST":
        nombre = request.form["nombre"]
        email = request.form["email"]
        password = request.form["password"]

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO users (nombre, email, password) VALUES (%s, %s, %s)",
                           (nombre, email, password))
            cursor.close()
            conn.close()
            flash("Usuario registrado con éxito", "success")
            return redirect(url_for("login"))
        except Exception as err:
            flash(f"Error: {err}", "danger")

    return render_template("register.html", node_info={"type": NODE_TYPE, "id": NODE_ID})

@app.route("/login", methods=["GET", "POST"])
def login():
    if NODE_TYPE != "gateway":
        return jsonify({"error": "Login solo disponible en gateway"}), 404
        
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

    return render_template("login.html", node_info={"type": NODE_TYPE, "id": NODE_ID})

@app.route("/logout")
def logout():
    if NODE_TYPE != "gateway":
        return jsonify({"error": "Logout solo disponible en gateway"}), 404
    session.clear()
    flash("Sesión cerrada", "info")
    return redirect(url_for("home"))

@app.route("/files")
def files():
    if NODE_TYPE != "gateway":
        return jsonify({"error": "Vista de archivos solo en gateway"}), 404
        
    if "user_id" not in session:
        flash("Debes iniciar sesión", "warning")
        return redirect(url_for("login"))

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT f.id, f.filename, u.nombre, f.upload_date, f.file_hash, 
                   f.file_size, f.storage_nodes
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

    return render_template("files.html", files=files_list, 
                         node_info={"type": NODE_TYPE, "id": NODE_ID, "storage_nodes": STORAGE_NODES})

@app.route("/upload", methods=["POST"])
def upload():
    if NODE_TYPE != "gateway":
        return jsonify({"error": "Upload solo disponible en gateway"}), 400
        
    if "user_id" not in session:
        flash("Debes iniciar sesión", "warning")
        return redirect(url_for("login"))

    file = request.files["file"]
    if not file or not file.filename:
        flash("No se seleccionó archivo", "warning")
        return redirect(url_for("files"))

    try:
        # Leer archivo y calcular hash
        file_content = file.read()
        file_hash = calculate_file_hash(file_content)
        filename = file.filename
        file_size = len(file_content)
        
        print(f"📤 [GATEWAY] Procesando upload: {filename} ({file_size} bytes)")
        
        # Seleccionar nodos de almacenamiento
        selected_nodes = select_storage_nodes(file_hash)
        print(f"📍 [GATEWAY] Nodos seleccionados: {selected_nodes}")
        
        # Replicar en todos los nodos seleccionados
        successful_replicas = 0
        failed_nodes = []
        
        for node in selected_nodes:
            if replicate_to_storage_node(node, file_content, filename, file_hash):
                successful_replicas += 1
            else:
                failed_nodes.append(node)
        
        # Verificar que tengamos al menos 2 réplicas exitosas
        if successful_replicas >= 2:
            # Guardar metadatos en BD centralizada
            storage_nodes_str = ",".join(selected_nodes)
            
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO files (filename, uploader_id, file_hash, file_size, storage_nodes) 
                VALUES (%s, %s, %s, %s, %s)
            """, (filename, session["user_id"], file_hash, file_size, storage_nodes_str))
            cursor.close()
            conn.close()
            
            flash(f"✅ Archivo replicado en {successful_replicas} nodos: {selected_nodes}", "success")
            if failed_nodes:
                flash(f"⚠️ Fallos en nodos: {failed_nodes}", "warning")
        else:
            flash(f"❌ Error: Solo {successful_replicas} réplicas exitosas. Mínimo requerido: 2", "danger")
            
    except Exception as e:
        flash(f"Error procesando archivo: {e}", "danger")

    return redirect(url_for("files"))

@app.route("/download/<file_id>")
def download_file(file_id):
    if NODE_TYPE != "gateway":
        return jsonify({"error": "Download solo disponible en gateway"}), 404
        
    if "user_id" not in session:
        flash("Debes iniciar sesión", "warning")
        return redirect(url_for("login"))
    
    try:
        # Obtener metadatos del archivo
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT filename, file_hash, storage_nodes, file_size 
            FROM files WHERE id = %s
        """, (file_id,))
        file_info = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not file_info:
            flash("Archivo no encontrado", "danger")
            return redirect(url_for("files"))
            
        filename, file_hash, storage_nodes_str, file_size = file_info
        storage_nodes = storage_nodes_str.split(",")
        
        print(f"📥 [GATEWAY] Descargando {filename} desde nodos: {storage_nodes}")
        
        # Intentar recuperar desde cada nodo hasta encontrar uno disponible
        file_content = None
        successful_node = None
        
        for node in storage_nodes:
            file_content = retrieve_from_storage_node(node, file_hash, filename)
            if file_content:
                successful_node = node
                break
        
        if file_content:
            print(f"✅ [GATEWAY] Archivo recuperado desde {successful_node}")
            return send_file(
                io.BytesIO(file_content),
                download_name=filename,
                as_attachment=True
            )
        else:
            flash("❌ Archivo no disponible en ningún nodo de almacenamiento", "danger")
            
    except Exception as e:
        flash(f"Error descargando archivo: {e}", "danger")
    
    return redirect(url_for("files"))

# =============================================================================
# RUTAS INTERNAS PARA COMUNICACIÓN ENTRE NODOS
# =============================================================================

@app.route("/internal/store", methods=["POST"])
def internal_store():
    """Endpoint interno para almacenar archivos (Solo nodos de storage)"""
    if NODE_TYPE != "storage":
        return jsonify({"error": "Este nodo no es de almacenamiento"}), 400
    
    try:
        file = request.files["file"]
        file_hash = request.form["file_hash"]
        filename = request.form["filename"]
        
        if not file or not file_hash or not filename:
            return jsonify({"error": "Parámetros faltantes"}), 400
        
        # Guardar archivo localmente
        local_filename = f"{file_hash}_{filename}"
        filepath = os.path.join(STORAGE_PATH, local_filename)
        
        file.save(filepath)
        
        print(f"💾 [STORAGE-{NODE_ID}] Archivo almacenado: {filepath}")
        
        return jsonify({
            "status": "success", 
            "node_id": NODE_ID,
            "filepath": local_filename,
            "size": os.path.getsize(filepath)
        }), 200
        
    except Exception as e:
        print(f"❌ [STORAGE-{NODE_ID}] Error almacenando: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/internal/retrieve/<filename>")
def internal_retrieve(filename):
    """Endpoint interno para servir archivos (Solo nodos de storage)"""
    if NODE_TYPE != "storage":
        return jsonify({"error": "Este nodo no es de almacenamiento"}), 400
    
    try:
        filepath = os.path.join(STORAGE_PATH, filename)
        
        if os.path.exists(filepath):
            print(f"📤 [STORAGE-{NODE_ID}] Sirviendo archivo: {filename}")
            return send_file(filepath)
        else:
            print(f"🔍 [STORAGE-{NODE_ID}] Archivo no encontrado: {filename}")
            return jsonify({"error": "Archivo no encontrado"}), 404
            
    except Exception as e:
        print(f"❌ [STORAGE-{NODE_ID}] Error sirviendo archivo: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/internal/status")
def internal_status():
    """Status interno del nodo"""
    try:
        storage_info = {}
        if NODE_TYPE == "storage":
            files = os.listdir(STORAGE_PATH) if os.path.exists(STORAGE_PATH) else []
            storage_info = {
                "storage_path": STORAGE_PATH,
                "files_count": len(files),
                "files": files[:10]  # Solo mostrar primeros 10
            }
        
        return jsonify({
            "node_id": NODE_ID,
            "node_type": NODE_TYPE,
            "status": "healthy",
            "storage_nodes": STORAGE_NODES if NODE_TYPE == "gateway" else [],
            **storage_info
        }), 200
        
    except Exception as e:
        return jsonify({
            "node_id": NODE_ID,
            "node_type": NODE_TYPE,
            "status": "error",
            "error": str(e)
        }), 500

@app.route("/health")
def health():
    """Health check público"""
    try:
        # Probar conexión a BD
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "node_id": NODE_ID,
            "node_type": NODE_TYPE,
            "database": "connected",
            "storage_nodes": STORAGE_NODES if NODE_TYPE == "gateway" else "N/A"
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "node_id": NODE_ID,
            "node_type": NODE_TYPE,
            "error": str(e)
        }), 500

if __name__ == "__main__":
    print(f"🚀 Iniciando nodo distribuido...")
    print(f"📋 Tipo: {NODE_TYPE}")
    print(f"🆔 ID: {NODE_ID}")
    
    if NODE_TYPE == "gateway":
        print(f"🌐 Gateway iniciado - Puerto principal: 5000")
        print(f"💾 Nodos de almacenamiento: {STORAGE_NODES}")
    else:
        print(f"📁 Nodo de almacenamiento - Directorio: {STORAGE_PATH}")
    
    app.run(debug=True, host='0.0.0.0', port=5000)