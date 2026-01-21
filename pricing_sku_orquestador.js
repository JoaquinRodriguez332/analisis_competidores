// ============================================================================
// pricing_sku_orquestador.js - VERSIÓN SIMPLIFICADA (SOLO PART1)
// ============================================================================

const sql = require('mssql');
const { obtenerPool } = require('./conexion');  // ← CAMBIO AQUÍ
const part1 = require('./services/pricing_sku_part1_preparacion');

/**
 * Ejecuta el análisis completo de pricing SKU
 */
async function ejecutarAnalisisPricingSKU(filtros = {}) {
	const tiempoInicio = Date.now();
	
	try {
		console.log('🚀 Iniciando análisis de pricing SKU...');
		console.log('📋 Filtros aplicados:', filtros);
		
		// ====================================================================
		// EJECUTAR PART 1: Análisis completo en un solo query
		// ====================================================================
		console.log('⏳ Ejecutando Part 1 (análisis completo)...');
		const inicioPart1 = Date.now();
		
		const pool = obtenerPool();  // ← CAMBIO AQUÍ
		const request = pool.request();
		
		// Configurar parámetros
		request.input('TiendaParamInput', sql.NVarChar(100), filtros.tienda || null);
		request.input('CategoriaParamInput', sql.NVarChar(200), filtros.categoria || null);
		request.input('CategoriaN2ParamInput', sql.NVarChar(200), filtros.categoriaN2 || null);
		request.input('CategoriaN3ParamInput', sql.NVarChar(200), filtros.categoriaN3 || null);
		request.input('MarcaParamInput', sql.NVarChar(100), filtros.marca || null);
		request.input('BuscarParamInput', sql.NVarChar(200), filtros.buscar || null);
		request.input('FechaInicioInput', sql.DateTime, filtros.fechaInicio || null);
		request.input('FechaFinInput', sql.DateTime, filtros.fechaFin || null);
		request.input('PeriodoParamInput', sql.VarChar(10), filtros.periodo || '30D');
		
		// Ejecutar query completo
		const resultado = await request.query(part1.query);
		
		const tiempoPart1 = Date.now() - inicioPart1;
		const cantidadProductos = resultado.recordset.length;
		
		console.log(`✅ Part 1 completada: ${cantidadProductos} productos en ${tiempoPart1}ms`);
		
		// ====================================================================
		// PROCESAR RESULTADOS
		// ====================================================================
		const productos = resultado.recordset;
		
		// Contar productos por status
		const analisisStatus = productos.reduce((acc, p) => {
			acc[p.Status] = (acc[p.Status] || 0) + 1;
			return acc;
		}, {});
		
		// ====================================================================
		// RESPUESTA FINAL
		// ====================================================================
		const tiempoTotal = Date.now() - tiempoInicio;
		
		return {
			success: true,
			data: productos,
			metadata: {
				part1: {
					filasGeneradas: cantidadProductos,
					tiempoMs: tiempoPart1,
					estado: 'completada'
				},
				analisisStatus,
				tiempoTotalMs: tiempoTotal,
				filtrosAplicados: filtros,
				fechaEjecucion: new Date().toISOString()
			},
			message: `Análisis completado. ${cantidadProductos} productos encontrados.`
		};
		
	} catch (error) {
		console.error('❌ Error en análisis de pricing SKU:', error);
		
		return {
			success: false,
			message: error.message || 'Error desconocido en el análisis',
			error: {
				mensaje: error.message,
				parte: 'desconocida',
				detalles: error.toString()
			}
		};
	}
}

module.exports = {
	ejecutarAnalisisPricingSKU
};