// ============================================================================
// routes_pricing_sku.js
// ============================================================================
// Rutas para el análisis de pricing SKU
// ============================================================================

const express = require('express');
const router = express.Router();

// Importamos AMBAS funciones del orquestador
const { 
    ejecutarAnalisisPricingSKU, 
    obtenerDetalleProducto 
} = require('../pricing_sku_orquestador');

// ============================================================================
// GET /api/pricing/sku/health - Health check
// ============================================================================
router.get('/health', (req, res) => {
    res.json({
        success: true,
        message: 'API de Pricing SKU operativa',
        timestamp: new Date().toISOString()
    });
});

// ============================================================================
// GET /api/pricing/sku - Obtener análisis de precios (Grilla Principal)
// ============================================================================
router.get('/', async (req, res) => {
    try {
        console.log('📥 Request recibida en /api/pricing/sku');
        
        // Extraer parámetros
        const {
            tienda,
            categoria,
            categoriaN2,
            categoriaN3,
            marca,
            buscar,
            sku,         // <--- Recibimos el SKU
            fechaInicio,
            fechaFin,
            periodo = '30D'
        } = req.query;
        
        // Validar que venga al menos un filtro
        if (!categoria && !tienda && !marca && !buscar && !sku) {
            return res.status(400).json({
                success: false,
                message: 'Debe especificar al menos un filtro: categoria, tienda, marca, buscar o sku'
            });
        }
        
        // Construir filtros
        const filtros = {
            tienda: tienda || null,
            categoria: categoria || null,
            categoriaN2: categoriaN2 || null,
            categoriaN3: categoriaN3 || null,
            marca: marca || null,
            // Aquí está la magia: Si viene SKU, lo usamos como búsqueda
            buscar: buscar || sku || null, 
            fechaInicio: fechaInicio ? new Date(fechaInicio) : null,
            fechaFin: fechaFin ? new Date(fechaFin) : null,
            periodo: periodo
        };
        
        console.log('🔍 Filtros:', `categoria=${categoria}, marca=${marca}, sku=${sku}, buscar=${filtros.buscar}`);
        
        // Ejecutar análisis principal (Part 1)
        const resultado = await ejecutarAnalisisPricingSKU(filtros);
        
        res.json(resultado);
        
    } catch (error) {
        console.error('❌ Error en endpoint GET /api/pricing/sku:', error);
        res.status(500).json({
            success: false,
            message: 'Error interno del servidor',
            error: error.message
        });
    }
});

// ============================================================================
// GET /api/pricing/sku/detalle/:sku - Ver TODOS los competidores de un producto
// ============================================================================
router.get('/detalle/:sku', async (req, res) => {
    try {
        const { sku } = req.params;

        if (!sku) {
            return res.status(400).json({
                success: false,
                message: 'El SKU es obligatorio para ver el detalle'
            });
        }

        console.log(`📥 Solicitud de detalle (drill-down) para SKU: ${sku}`);

        // Ejecutar análisis de detalle (Part 2)
        const resultado = await obtenerDetalleProducto(sku);

        if (resultado.success) {
            res.json(resultado);
        } else {
            res.status(500).json(resultado);
        }

    } catch (error) {
        console.error('❌ Error en endpoint GET /api/pricing/sku/detalle:', error);
        res.status(500).json({
            success: false,
            message: 'Error interno obteniendo detalle',
            error: error.message
        });
    }
});

// ============================================================================
// GET /api/pricing/sku/stats - Obtener solo estadísticas rápidas
// ============================================================================
router.get('/stats', async (req, res) => {
    try {
        const filtros = {
            tienda: req.query.tienda || null,
            categoria: req.query.categoria || null,
            marca: req.query.marca || null,
            periodo: req.query.periodo || '30D'
        };
        
        if (!filtros.categoria && !filtros.tienda && !filtros.marca) {
            return res.status(400).json({
                success: false,
                message: 'Debe especificar al menos un filtro: categoria, tienda o marca'
            });
        }
        
        const resultado = await ejecutarAnalisisPricingSKU(filtros);
        
        if (resultado.success) {
            res.json({
                success: true,
                stats: resultado.metadata.analisisStatus,
                metadata: {
                    totalProductos: resultado.data.length,
                    tiempoMs: resultado.metadata.tiempoTotalMs,
                    filtros: resultado.metadata.filtrosAplicados
                }
            });
        } else {
            res.status(500).json(resultado);
        }
        
    } catch (error) {
        console.error('❌ Error en endpoint GET /api/pricing/sku/stats:', error);
        res.status(500).json({
            success: false,
            message: 'Error interno del servidor',
            error: error.message
        });
    }
});

// ============================================================================
// GET /api/pricing/sku/export - Exportar a CSV
// ============================================================================
router.get('/export', async (req, res) => {
    try {
        const formato = req.query.formato || 'csv';
        
        const filtros = {
            tienda: req.query.tienda || null,
            categoria: req.query.categoria || null,
            marca: req.query.marca || null,
            periodo: req.query.periodo || '30D'
        };
        
        if (!filtros.categoria && !filtros.tienda && !filtros.marca) {
            return res.status(400).json({
                success: false,
                message: 'Debe especificar al menos un filtro: categoria, tienda o marca'
            });
        }
        
        const resultado = await ejecutarAnalisisPricingSKU(filtros);
        
        if (resultado.success && resultado.data.length > 0) {
            if (formato === 'csv') {
                const campos = Object.keys(resultado.data[0]);
                const csv = [
                    campos.join(','),
                    ...resultado.data.map(row => 
                        campos.map(campo => 
                            typeof row[campo] === 'string' && row[campo].includes(',') 
                                ? `"${row[campo]}"` 
                                : row[campo]
                        ).join(',')
                    )
                ].join('\n');
                
                res.setHeader('Content-Type', 'text/csv');
                res.setHeader('Content-Disposition', `attachment; filename="pricing_sku_${Date.now()}.csv"`);
                res.send(csv);
                
            } else {
                res.status(501).json({
                    success: false,
                    message: 'Formato no soportado (solo csv)'
                });
            }
        } else {
            res.status(404).json({
                success: false,
                message: 'No hay datos para exportar'
            });
        }
        
    } catch (error) {
        console.error('❌ Error en endpoint export:', error);
        res.status(500).json({
            success: false,
            message: 'Error interno',
            error: error.message
        });
    }
});

module.exports = router;