// ============================================================================
// routes_pricing_sku.js
// ============================================================================
// Rutas para el análisis de pricing SKU
// ============================================================================

const express = require('express');
const router = express.Router();
const { ejecutarAnalisisPricingSKU } = require('../pricing_sku_orquestador');

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
// GET /api/pricing/sku - Obtener análisis de precios por SKU
// ============================================================================
router.get('/', async (req, res) => {
	try {
		console.log('📥 Request recibida en /api/pricing/sku');
		
		// Extraer parámetros de query
		const {
			tienda,
			categoria,
			categoriaN2,
			categoriaN3,
			marca,
			buscar,
			fechaInicio,
			fechaFin,
			periodo = '30D'
		} = req.query;
		
		// Validar que al menos un filtro esté presente
		if (!categoria && !tienda && !marca && !buscar) {
			return res.status(400).json({
				success: false,
				message: 'Debe especificar al menos un filtro: categoria, tienda, marca o buscar'
			});
		}
		
		// Construir objeto de filtros
		const filtros = {
			tienda: tienda || null,
			categoria: categoria || null,
			categoriaN2: categoriaN2 || null,
			categoriaN3: categoriaN3 || null,
			marca: marca || null,
			buscar: buscar || null,
			fechaInicio: fechaInicio ? new Date(fechaInicio) : null,
			fechaFin: fechaFin ? new Date(fechaFin) : null,
			periodo: periodo
		};
		
		console.log('🔍 Filtros:', `categoria=${categoria}, tienda=${tienda}, marca=${marca}, periodo=${periodo}`);
		
		// Ejecutar análisis
		const resultado = await ejecutarAnalisisPricingSKU(filtros);
		
		// Retornar resultado
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
		
		// Validar que al menos un filtro esté presente
		if (!filtros.categoria && !filtros.tienda && !filtros.marca) {
			return res.status(400).json({
				success: false,
				message: 'Debe especificar al menos un filtro: categoria, tienda o marca'
			});
		}
		
		const resultado = await ejecutarAnalisisPricingSKU(filtros);
		
		if (resultado.success) {
			// Retornar solo estadísticas
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
// GET /api/pricing/sku/export - Exportar a CSV/Excel
// ============================================================================
router.get('/export', async (req, res) => {
	try {
		const formato = req.query.formato || 'csv'; // csv o excel
		
		const filtros = {
			tienda: req.query.tienda || null,
			categoria: req.query.categoria || null,
			marca: req.query.marca || null,
			periodo: req.query.periodo || '30D'
		};
		
		// Validar que al menos un filtro esté presente
		if (!filtros.categoria && !filtros.tienda && !filtros.marca) {
			return res.status(400).json({
				success: false,
				message: 'Debe especificar al menos un filtro: categoria, tienda o marca'
			});
		}
		
		const resultado = await ejecutarAnalisisPricingSKU(filtros);
		
		if (resultado.success && resultado.data.length > 0) {
			if (formato === 'csv') {
				// Convertir a CSV
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
				
			} else if (formato === 'excel') {
				// Aquí podrías usar una librería como exceljs
				res.status(501).json({
					success: false,
					message: 'Exportación a Excel no implementada aún. Use formato=csv'
				});
			} else {
				res.status(400).json({
					success: false,
					message: 'Formato inválido. Use: csv o excel'
				});
			}
		} else {
			res.status(404).json({
				success: false,
				message: 'No hay datos para exportar'
			});
		}
		
	} catch (error) {
		console.error('❌ Error en endpoint GET /api/pricing/sku/export:', error);
		res.status(500).json({
			success: false,
			message: 'Error interno del servidor',
			error: error.message
		});
	}
});

module.exports = router;