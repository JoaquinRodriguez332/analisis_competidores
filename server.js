const express = require('express');

const { conectar, desconectar } = require('./conexion');

const cors = require('cors');
require('dotenv').config();

const routesPricing = require('./routes/routes_pricing_sku');


const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Rutas
app.use('/api/pricing', routesPricing);


// Ruta de prueba
app.get('/', (req, res) => {
  res.json({ 
    mensaje: 'API funcionando ✅',
    version: '1.0.0',
    endpoints: [
      'GET /api/pricing/sku',
      'GET /api/pricing/sku/:idProductoMimbral',
    ]
  });
});

const PORT = process.env.PORT || 3000;

async function iniciar() {
  try {
    const pool = await conectar();
    app.locals.sqlPool = pool;  // ← CRUCIAL
    
    app.listen(PORT, () => {
      console.log(`✅ Servidor corriendo en http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  }
}

iniciar();