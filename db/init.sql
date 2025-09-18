USE reto_db;

-- Tabla de usuarios
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_email (email)
);

-- Tabla de archivos con metadatos distribuidos
CREATE TABLE files (
    id INT AUTO_INCREMENT PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    uploader_id INT,
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_hash VARCHAR(64) NOT NULL UNIQUE,  -- Hash único para cada archivo
    file_size BIGINT NOT NULL,
    storage_nodes TEXT NOT NULL,  -- Lista de nodos donde está replicado
    status ENUM('active', 'corrupted', 'deleted') DEFAULT 'active',
    replica_count INT DEFAULT 0,
    FOREIGN KEY (uploader_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX idx_file_hash (file_hash),
    INDEX idx_uploader (uploader_id),
    INDEX idx_status (status)
);

-- Usuario de prueba
INSERT INTO users (nombre, email, password) VALUES 
('Admin', 'admin@test.com', 'admin123'),
('Usuario', 'user@test.com', 'user123');

-- Logs de replicación (opcional para debugging)
CREATE TABLE replication_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_hash VARCHAR(64) NOT NULL,
    source_node VARCHAR(50) NOT NULL,
    target_node VARCHAR(50) NOT NULL,
    operation ENUM('replicate', 'retrieve', 'delete') NOT NULL,
    status ENUM('success', 'failed') NOT NULL,
    error_message TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_file_hash_log (file_hash),
    INDEX idx_operation (operation),
    INDEX idx_status_log (status)
);