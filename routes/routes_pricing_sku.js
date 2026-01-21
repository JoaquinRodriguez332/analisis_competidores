const express = require('express');
const router = express.Router();
const { obtenerAnalizisPreciosSKU, obtenerEstadisticasRapidas } = require('../pricing_sku_orquestador');
const ExcelJS = require('exceljs');
const { Parser } = require('json2csv');


router.get('/sku', async (req, res) => {
	try {
		// Validar que tengamos pool de conexión
		const pool = req.app.locals.sqlPool;
		if (!pool) {
			return res.status(500).json({
				success: false,
				message: 'Conexión SQL no disponible'
			});
		}

		// Extraer parámetros
		const {
			categoria,
			tienda,
			marca,
			buscar,
			periodo = '30D',
			fechaInicio,
			fechaFin
		} = req.query;

		// Validar período
		const periodosValidos = ['7D', '30D', '60D', '90D'];
		if (!periodosValidos.includes(periodo)) {
			return res.status(400).json({
				success: false,
				message: `Período inválido. Válidos: ${periodosValidos.join(', ')}`
			});
		}

		// Log de request
		console.log(`🔍 [ROUTES] GET /api/pricing/sku`);
		console.log(`   Filtros: categoria=${categoria}, tienda=${tienda}, marca=${marca}, periodo=${periodo}`);

		// Ejecutar orquestador
		const resultado = await obtenerAnalizisPreciosSKU(pool, {
			categoria,
			tienda,
			marca,
			buscar,
			periodo,
			fechaInicio,
			fechaFin
		});

		if (!resultado.success) {
			console.error('❌ Error en orquestador:', resultado.error);
			return res.status(500).json({
				success: false,
				message: resultado.error.mensaje,
				error: resultado.error
			});
		}

		// Response exitoso
		res.json({
			success: true,
			data: resultado.data,
			metadata: resultado.metadata,
			message: `Análisis completado. ${resultado.data.length} productos encontrados.`
		});

	} catch (error) {
		console.error('❌ Error en endpoint GET /api/pricing/sku:', error);
		res.status(500).json({
			success: false,
			message: 'Error interno del servidor',
			error: error.message
		});
	}
});

router.get('/sku/export', async (req, res) => {
	try {
		const pool = req.app.locals.sqlPool;
		if (!pool) {
			return res.status(500).json({
				success: false,
				message: 'Conexión SQL no disponible'
			});
		}

		// Extraer parámetros
		const {
			formato = 'csv',
			categoria,
			tienda,
			marca,
			buscar,
			periodo = '30D',
			fechaInicio,
			fechaFin
		} = req.query;

		// Validar formato
		if (!['csv', 'excel'].includes(formato)) {
			return res.status(400).json({
				success: false,
				message: 'Formato inválido. Use: csv o excel'
			});
		}

		console.log(`📥 [ROUTES] GET /api/pricing/sku/export (${formato})`);

		// Ejecutar orquestador
		const resultado = await obtenerAnalizisPreciosSKU(pool, {
			categoria,
			tienda,
			marca,
			buscar,
			periodo,
			fechaInicio,
			fechaFin
		});

		if (!resultado.success) {
			return res.status(500).json({
				success: false,
				message: resultado.error.mensaje
			});
		}

		const timestamp = new Date().toISOString().split('T')[0].replace(/-/g, '');
		const filename = `pricing_sku_${timestamp}`;

		// Exportar según formato
		if (formato === 'csv') {
			const csv = new Parser().parse(resultado.data);
			res.setHeader('Content-Type', 'text/csv');
			res.setHeader('Content-Disposition', `attachment; filename="${filename}.csv"`);
			res.send(csv);

		} else if (formato === 'excel') {
			const workbook = new ExcelJS.Workbook();
			const worksheet = workbook.addWorksheet('Análisis Precios');

			// Headers
			const columns = Object.keys(resultado.data[0] || {});
			worksheet.columns = columns.map(col => ({ header: col, key: col }));

			// Datos
			resultado.data.forEach(row => worksheet.addRow(row));

			// Estilos
			worksheet.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
			worksheet.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF366092' } };

			// Auto-width
			worksheet.columns.forEach(col => {
				const maxLength = Math.max(
					col.header.length,
					...(resultado.data.map(row => String(row[col.key] || '').length))
				);
				col.width = Math.min(maxLength + 2, 50);
			});

			res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
			res.setHeader('Content-Disposition', `attachment; filename="${filename}.xlsx"`);

			await workbook.xlsx.write(res);
			res.end();
		}

	} catch (error) {
		console.error('❌ Error en endpoint GET /api/pricing/sku/export:', error);
		res.status(500).json({
			success: false,
			message: 'Error al exportar',
			error: error.message
		});
	}
});

/**
 * ENDPOINT 3: GET /api/pricing/sku/stats
 * Estadísticas rápidas (sin datos detallados)
 * 
 * Query params:
 *   - (mismos filtros)
 * 
 * Response:
 *   {
 *     success: boolean,
 *     stats: {
 *       totalProductos: number,
 *       porStatus: { "Más Barato": 10, "Competitivo": 50, ... },
 *       tiempoMs: number
 *     }
 *   }
 */
router.get('/sku/stats', async (req, res) => {
	try {
		const pool = req.app.locals.sqlPool;
		if (!pool) {
			return res.status(500).json({
				success: false,
				message: 'Conexión SQL no disponible'
			});
		}

		const {
			categoria,
			tienda,
			marca,
			buscar,
			periodo = '30D',
			fechaInicio,
			fechaFin
		} = req.query;

		console.log(`📊 [ROUTES] GET /api/pricing/sku/stats`);

		// Ejecutar (devuelve solo estadísticas)
		const stats = await obtenerEstadisticasRapidas(pool, {
			categoria,
			tienda,
			marca,
			buscar,
			periodo,
			fechaInicio,
			fechaFin
		});

		if (stats.error) {
			return res.status(500).json({
				success: false,
				message: 'Error al obtener estadísticas',
				error: stats.error
			});
		}

		res.json({
			success: true,
			stats
		});

	} catch (error) {
		console.error('❌ Error en endpoint GET /api/pricing/sku/stats:', error);
		res.status(500).json({
			success: false,
			message: 'Error interno del servidor',
			error: error.message
		});
	}
});

/**
 * ENDPOINT 4: GET /api/pricing/sku/health
 * Health check del servicio
 */
router.get('/sku/health', (req, res) => {
	res.json({
		success: true,
		status: 'ok',
		service: 'pricing-sku-analysis',
		timestamp: new Date().toISOString()
	});
});

module.exports = router;
