
const part1 = require('./services/pricing_sku_part1_preparacion');
const part2 = require('./services/pricing_sku_part2_agregacion');
const part3 = require('./services/pricing_sku_part3_resultados');

/**
 * FUNCIÓN PRINCIPAL: obtenerAnalizisPreciosSKU
 * 
 * Parámetros:
 *   - pool: Connection pool de SQL Server
 *   - filtros: { categoria, tienda, marca, buscar, periodo, fechaInicio, fechaFin }
 * 
 * Retorna:
 *   - { success, data, metadata, error }
 */
async function obtenerAnalizisPreciosSKU(pool, filtros) {
	const inicio = Date.now();
	let resultadoFinal = [];
	let metadata = {};

	try {
		// ====================================================================
		// VALIDAR PARÁMETROS
		// ====================================================================
		if (!pool) {
			throw new Error('Pool de conexión SQL no proporcionado');
		}

		const {
			categoria = null,
			tienda = null,
			marca = null,
			buscar = null,
			periodo = '30D',
			fechaInicio = null,
			fechaFin = null
		} = filtros;

		console.log('🔄 [ORQUESTADOR] Iniciando análisis de precios');
		console.log(`📋 Filtros: ${JSON.stringify(filtros)}`);

		// ====================================================================
		// PART 1: PREPARACIÓN - JOINs + Filtros + Históricos
		// ====================================================================
		console.log('📍 [PART 1] Ejecutando preparación...');
		const tiempoP1 = Date.now();

		const resultadoP1 = await pool.request()
			.input('TiendaParamInput', tienda)
			.input('CategoriaParamInput', categoria)
			.input('CategoriaN2ParamInput', null)
			.input('CategoriaN3ParamInput', null)
			.input('MarcaParamInput', marca)
			.input('BuscarParamInput', buscar)
			.input('FechaInicioInput', fechaInicio)
			.input('FechaFinInput', fechaFin)
			.input('PeriodoParamInput', periodo)
			.query(part1.query);

		const rawData = resultadoP1.recordset;
		const tiempoP1Ejecutado = Date.now() - tiempoP1;

		console.log(`✅ [PART 1] Completada en ${tiempoP1Ejecutado}ms`);
		console.log(`   Filas generadas: ${rawData.length}`);

		metadata.part1 = {
			filasGeneradas: rawData.length,
			tiempoMs: tiempoP1Ejecutado,
			estado: 'completada'
		};

		if (rawData.length === 0) {
			console.warn('⚠️  [PART 1] No hay datos con los filtros especificados');
			return {
				success: true,
				data: [],
				metadata: {
					...metadata,
					mensaje: 'Sin datos para los filtros especificados',
					tiempoTotalMs: Date.now() - inicio
				}
			};
		}

		// ====================================================================
		// PART 2: AGREGACIÓN - Window Functions + Deduplicación
		// ====================================================================
		console.log('📍 [PART 2] Ejecutando agregación...');
		const tiempoP2 = Date.now();

		// Convertir RawData a JSON para Part 2
		const rawDataJSON = JSON.stringify(rawData);

		const resultadoP2 = await pool.request()
			.input('RawDataParamInput', rawDataJSON)
			.query(part2.query(rawData));

		const productosMimbra = resultadoP2.recordset;
		const tiempoP2Ejecutado = Date.now() - tiempoP2;

		console.log(`✅ [PART 2] Completada en ${tiempoP2Ejecutado}ms`);
		console.log(`   Filas generadas: ${productosMimbra.length}`);

		metadata.part2 = {
			filasGeneradas: productosMimbra.length,
			tiempoMs: tiempoP2Ejecutado,
			tasaDeduplicacion: ((rawData.length - productosMimbra.length) / rawData.length * 100).toFixed(2) + '%',
			estado: 'completada'
		};

		// ====================================================================
		// PART 3: RESULTADOS - KPIs Finales + Status
		// ====================================================================
		console.log('📍 [PART 3] Ejecutando cálculos finales...');
		const tiempoP3 = Date.now();

		// Convertir ProductosMimbra a JSON para Part 3
		const productosMimbraJSON = JSON.stringify(productosMimbra);

		const resultadoP3 = await pool.request()
			.input('ProductosMimbraInput', productosMimbraJSON)
			.query(part3.query(productosMimbra));

		resultadoFinal = resultadoP3.recordset;
		const tiempoP3Ejecutado = Date.now() - tiempoP3;

		console.log(`✅ [PART 3] Completada en ${tiempoP3Ejecutado}ms`);
		console.log(`   Filas finales: ${resultadoFinal.length}`);

		metadata.part3 = {
			filasGeneradas: resultadoFinal.length,
			tiempoMs: tiempoP3Ejecutado,
			estado: 'completada'
		};

		// ====================================================================
		// ANÁLISIS DE STATUS (para metadata)
		// ====================================================================
		const analisisStatus = resultadoFinal.reduce((acc, prod) => {
			acc[prod.Status] = (acc[prod.Status] || 0) + 1;
			return acc;
		}, {});

		metadata.analisisStatus = analisisStatus;

		// ====================================================================
		// RESPONSE FINAL
		// ====================================================================
		const tiempoTotal = Date.now() - inicio;

		console.log('✅ [ORQUESTADOR] Análisis completado');
		console.log(`⏱️  Tiempo total: ${tiempoTotal}ms`);

		return {
			success: true,
			data: resultadoFinal,
			metadata: {
				...metadata,
				tiempoTotalMs: tiempoTotal,
				filtrosAplicados: filtros,
				fechaEjecucion: new Date().toISOString()
			}
		};

	} catch (error) {
		console.error('❌ [ORQUESTADOR] Error en cascada:', error.message);

		return {
			success: false,
			data: [],
			error: {
				mensaje: error.message,
				parte: error.parte || 'desconocida',
				detalles: error.originalError?.message || error.message
			},
			metadata: {
				tiempoTotalMs: Date.now() - inicio,
				estado: 'error'
			}
		};
	}
}

/**
 * FUNCIÓN AUXILIAR: obtenerEstadisticasRapidas
 * Retorna solo conteos por status (más rápido)
 */
async function obtenerEstadisticasRapidas(pool, filtros) {
	try {
		const resultado = await obtenerAnalizisPreciosSKU(pool, filtros);
		
		if (!resultado.success) {
			return { error: resultado.error };
		}

		const stats = {
			totalProductos: resultado.data.length,
			porStatus: resultado.metadata.analisisStatus,
			tiempoMs: resultado.metadata.tiempoTotalMs
		};

		return stats;
	} catch (error) {
		return { error: error.message };
	}
}

module.exports = {
	obtenerAnalizisPreciosSKU,
	obtenerEstadisticasRapidas
};
