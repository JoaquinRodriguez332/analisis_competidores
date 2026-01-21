const sql = require('mssql');
require('dotenv').config();

const config = {
  server: process.env.DB_SERVER,
  database: process.env.DB_DATABASE,
  authentication: {
    type: 'default',
    options: {
      userName: process.env.DB_USER,
      password: process.env.DB_PASSWORD
    }
  },
  options: {
    encrypt: true,
    trustServerCertificate: true,
    port: parseInt(process.env.DB_PORT) || 1433,
    requestTimeout: 30000
  }
};

let pool = null;

async function conectar() {
  try {
    if (!pool) {
      pool = new sql.ConnectionPool(config);
      await pool.connect();
      console.log('✅ Conectado a SQL Server');
    }
    return pool;
  } catch (error) {
    console.error('❌ Error:', error.message);
    pool = null;
    throw error;
  }
}

function obtenerPool() {
  if (!pool) throw new Error('Pool no inicializado');
  return pool;
}

async function desconectar() {
  if (pool) {
    await pool.close();
    pool = null;
    console.log('✅ Desconectado');
  }
}

module.exports = { conectar, obtenerPool, desconectar, config };

