// ============================================================================
// pricing_sku_part2_agregacion.js
// ============================================================================
// PARTE 2: Agregación de métricas competitivas
// INPUT: RawData (de Part 1)
// - Calcula mín/máx/cantidad de competidores
// - Calcula ranking y posición de nuestro precio
// - Deduplica a 1 fila por SKU Mimbral
// OUTPUT: ProductosMimbraUnicos con todas las métricas agregadas
// ============================================================================

// Esta función recibe el RawData de Part 1 como parámetro
const query = (rawDataArray) => {
	// Convertir el array de RawData en una tabla virtual temporal
	// Esto simula que ya tenemos los datos en memoria
	
	return `
-- ============================================================================
-- TABLA VIRTUAL: Importar RawData desde Part 1
-- ============================================================================
-- En una ejecución real, esto vendría de Part 1
-- Para testing, puedes pasar los datos como parámetro JSON

DECLARE @RawDataJSON NVARCHAR(MAX) = @RawDataParamInput;

-- Crear tabla temporal con los datos de RawData
IF OBJECT_ID('tempdb..#RawData') IS NOT NULL DROP TABLE #RawData;

CREATE TABLE #RawData (
	id_producto_mimbral INT,
	sku_mimbral NVARCHAR(100),
	nombre_mimbral NVARCHAR(500),
	marca_mimbral NVARCHAR(200),
	categoria NVARCHAR(200),
	categoriaN2 NVARCHAR(200),
	categoriaN3 NVARCHAR(200),
	url_mimbral NVARCHAR(MAX),
	imageurl_mimbral NVARCHAR(MAX),
	marketplace_mimbral NVARCHAR(100),
	precio_nuestro DECIMAL(18,2),
	fecha_precio_nuestro DATETIME,
	id_producto_externo INT,
	tienda NVARCHAR(100),
	sku_externo NVARCHAR(100),
	nombre_externo NVARCHAR(500),
	marca_externa NVARCHAR(200),
	url_externa NVARCHAR(MAX),
	imageurl_externa NVARCHAR(MAX),
	stock_competidor INT,
	precio_competidor DECIMAL(18,2),
	fecha_precio_competidor DATETIME
);

-- Insertar datos (en producción, estos vienen del INSERT de Part 1)
-- INSERT INTO #RawData SELECT * FROM Part1Results;

-- ============================================================================
-- CTE: CompetitividadAgregada
-- Calcula window functions sin conflictos:
-- - Todas las columnas se calculan por PARTITION BY id_producto_mimbral
-- - NO hay mezcla de columnas detalladas por competidor
-- ============================================================================
;WITH CompetitividadAgregada AS (
	SELECT
		r.id_producto_mimbral,
		r.sku_mimbral,
		r.nombre_mimbral,
		r.marca_mimbral,
		r.categoria,
		r.categoriaN2,
		r.categoriaN3,
		r.url_mimbral,
		r.imageurl_mimbral,
		r.marketplace_mimbral,
		r.precio_nuestro,
		r.fecha_precio_nuestro,
		
		-- MÍNIMO PRECIO de competencia
		MIN(NULLIF(r.precio_competidor, 0))
			OVER(PARTITION BY r.id_producto_mimbral) AS precio_min_competidor,
		
		-- MÁXIMO PRECIO de competencia
		MAX(NULLIF(r.precio_competidor, 0))
			OVER(PARTITION BY r.id_producto_mimbral) AS precio_max_competidor,
		
		-- CANTIDAD DE COMPETIDORES ÚNICOS
		COUNT(DISTINCT CASE 
			WHEN r.precio_competidor IS NOT NULL 
			AND r.precio_competidor > 0 
			THEN r.tienda 
		END)
			OVER(PARTITION BY r.id_producto_mimbral) AS cantidad_competidores,
		
		-- CONTAR cuántos competidores son más baratos que nosotros
		SUM(CASE
			WHEN r.precio_competidor > 0
			AND r.precio_competidor < r.precio_nuestro THEN 1
			ELSE 0
		END)
			OVER(PARTITION BY r.id_producto_mimbral) AS cantidad_mas_baratos,
		
		-- IDENTIFICAR tienda más barata
		MIN(CASE WHEN r.precio_competidor IS NOT NULL AND r.precio_competidor > 0
			THEN r.tienda ELSE NULL END)
			OVER(
				PARTITION BY r.id_producto_mimbral
				ORDER BY r.precio_competidor ASC
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS tienda_mas_barata,
		
		-- IDENTIFICAR tienda más cara
		MIN(CASE WHEN r.precio_competidor IS NOT NULL AND r.precio_competidor > 0
			THEN r.tienda ELSE NULL END)
			OVER(
				PARTITION BY r.id_producto_mimbral
				ORDER BY r.precio_competidor DESC
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS tienda_mas_cara
		
	FROM #RawData r
),

-- ============================================================================
-- CTE: ProductosMimbraUnicos
-- DEDUPLICAR: 1 fila por SKU Mimbral (eliminar filas repetidas por competidor)
-- ============================================================================
ProductosMimbraUnicos AS (
	SELECT DISTINCT
		id_producto_mimbral,
		sku_mimbral,
		nombre_mimbral,
		marca_mimbral,
		categoria,
		categoriaN2,
		categoriaN3,
		url_mimbral,
		imageurl_mimbral,
		marketplace_mimbral,
		precio_nuestro,
		fecha_precio_nuestro,
		precio_min_competidor,
		precio_max_competidor,
		cantidad_competidores,
		cantidad_mas_baratos,
		tienda_mas_barata,
		tienda_mas_cara
	FROM CompetitividadAgregada
)

-- ============================================================================
-- SELECT FINAL: Exportar ProductosMimbraUnicos para Part 3
-- ============================================================================
SELECT
	id_producto_mimbral,
	sku_mimbral,
	nombre_mimbral,
	marca_mimbral,
	categoria,
	categoriaN2,
	categoriaN3,
	url_mimbral,
	imageurl_mimbral,
	marketplace_mimbral,
	precio_nuestro,
	fecha_precio_nuestro,
	precio_min_competidor,
	precio_max_competidor,
	cantidad_competidores,
	cantidad_mas_baratos,
	tienda_mas_barata,
	tienda_mas_cara
FROM ProductosMimbraUnicos
ORDER BY categoria, nombre_mimbral
OPTION (RECOMPILE);
`;
};

module.exports = {
	query,
	description: 'PARTE 2: Agregación de métricas (window functions)',
	parameters: [
		{ name: '@RawDataParamInput', type: 'NVARCHAR(MAX)', nullable: false }
	]
};
