
const query = (productosMimbraArray) => {
	return `
-- ============================================================================
-- TABLA TEMPORAL: Importar ProductosMimbraUnicos desde Part 2
-- ============================================================================

IF OBJECT_ID('tempdb..#ProductosMimbraUnicos') IS NOT NULL 
	DROP TABLE #ProductosMimbraUnicos;

CREATE TABLE #ProductosMimbraUnicos (
	id_producto_mimbral INT PRIMARY KEY,
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
	precio_min_competidor DECIMAL(18,2),
	precio_max_competidor DECIMAL(18,2),
	cantidad_competidores INT,
	cantidad_mas_baratos INT,
	tienda_mas_barata NVARCHAR(100),
	tienda_mas_cara NVARCHAR(100)
);

-- INSERT INTO #ProductosMimbraUnicos SELECT * FROM Part2Results;

-- ============================================================================
-- CTE: ResultadoFinal
-- Calcula todas las métricas finales en una sola pasada:
-- 1. Delta % vs más barato
-- 2. Posición en ranking
-- 3. Status categorizado
-- ============================================================================
;WITH ResultadoFinal AS (
	SELECT
		-- Identidad del producto
		p.id_producto_mimbral,
		p.sku_mimbral,
		p.nombre_mimbral,
		p.marca_mimbral,
		p.categoria,
		p.categoriaN2,
		p.categoriaN3,
		p.url_mimbral,
		p.imageurl_mimbral,
		p.marketplace_mimbral,
		
		-- Precios
		p.precio_nuestro,
		p.precio_min_competidor,
		p.precio_max_competidor,
		p.fecha_precio_nuestro,
		
		-- ============================================================
		-- DELTA %: Comparación contra el más barato
		-- Fórmula: ((precio_nuestro - precio_min) / precio_min) * 100
		-- ============================================================
		CASE
			WHEN p.precio_min_competidor IS NULL 
				OR p.precio_min_competidor = 0
				OR p.cantidad_competidores = 0 THEN NULL
			ELSE ROUND(
				((p.precio_nuestro - p.precio_min_competidor) 
				/ p.precio_min_competidor) * 100, 2
			)
		END AS delta_vs_barato_porc,
		
		-- ============================================================
		-- POSICIÓN: 1 + cantidad_mas_baratos
		-- (cuántos competidores tienen precio más bajo)
		-- ============================================================
		CASE
			WHEN p.cantidad_competidores = 0 THEN NULL
			ELSE 1 + ISNULL(p.cantidad_mas_baratos, 0)
		END AS posicion_precio,
		
		-- ============================================================
		-- STATUS: Categorización basada en posición y delta
		-- - MÁS BARATO: posición = 1
		-- - COMPETITIVO: posición 2-3
		-- - RIESGO: posición > 3 Y delta <= 10%
		-- - MUY CARO: posición > 3 Y delta > 10%
		-- - SIN COMPETENCIA: 0 competidores
		-- ============================================================
		CASE
			WHEN p.cantidad_competidores = 0 THEN 'Sin Competencia'
			WHEN 1 + ISNULL(p.cantidad_mas_baratos, 0) = 1 THEN 'Más Barato'
			WHEN 1 + ISNULL(p.cantidad_mas_baratos, 0) BETWEEN 2 AND 3 
				THEN 'Competitivo'
			WHEN 1 + ISNULL(p.cantidad_mas_baratos, 0) > 3
				AND ROUND(
					((p.precio_nuestro - p.precio_min_competidor) 
					/ p.precio_min_competidor) * 100, 2
				) <= 10 THEN 'Riesgo'
			WHEN 1 + ISNULL(p.cantidad_mas_baratos, 0) > 3
				AND ROUND(
					((p.precio_nuestro - p.precio_min_competidor) 
					/ p.precio_min_competidor) * 100, 2
				) > 10 THEN 'Muy Caro'
			ELSE 'Sin Datos'
		END AS status,
		
		-- Competencia
		p.cantidad_competidores,
		p.cantidad_mas_baratos,
		p.tienda_mas_barata,
		p.tienda_mas_cara,
		
		-- Metadata
		GETDATE() AS fecha_analisis
	FROM #ProductosMimbraUnicos p
)

-- ============================================================================
-- SELECT FINAL: Tabla de resultados lista para API
-- ============================================================================
SELECT
	id_producto_mimbral AS IdProductoMimbral,
	sku_mimbral AS SKU,
	nombre_mimbral AS Nombre,
	marca_mimbral AS Marca,
	categoria AS Categoria,
	categoriaN2 AS CategoriaN2,
	categoriaN3 AS CategoriaN3,
	url_mimbral AS UrlMimbral,
	imageurl_mimbral AS ImagenMimbral,
	marketplace_mimbral AS MarketplaceMimbral,
	precio_nuestro AS PrecioNuestro,
	precio_min_competidor AS PrecioMinCompetencia,
	precio_max_competidor AS PrecioMaxCompetencia,
	delta_vs_barato_porc AS DeltaVsBaratoPorc,
	posicion_precio AS PosicionPrecio,
	cantidad_competidores AS CantidadCompetidores,
	cantidad_mas_baratos AS CantidadMasBaratos,
	tienda_mas_barata AS CompetidorMasBarato,
	tienda_mas_cara AS CompetidorMasCaro,
	status AS Status,
	fecha_precio_nuestro AS FechaPrecioNuestro,
	fecha_analisis AS FechaAnalisis
FROM ResultadoFinal
ORDER BY status DESC, delta_vs_barato_porc DESC, nombre_mimbral ASC
OPTION (RECOMPILE);
`;
};

module.exports = {
	query,
	description: 'PARTE 3: Cálculos finales y KPIs (Delta, Posición, Status)',
	parameters: [
		{ name: '@ProductosMimbraInput', type: 'NVARCHAR(MAX)', nullable: false }
	],
	
	// Referencia de Status Categories (para frontend)
	statusCategories: {
		'Más Barato': {
			icon: '🟢',
			color: '#22c55e',
			description: 'Nuestro precio es el más bajo entre competidores'
		},
		'Competitivo': {
			icon: '🔵',
			color: '#3b82f6',
			description: 'Posición 2-3, precios competitivos'
		},
		'Riesgo': {
			icon: '🟡',
			color: '#eab308',
			description: 'Posición > 3, pero diferencia <= 10%'
		},
		'Muy Caro': {
			icon: '🔴',
			color: '#ef4444',
			description: 'Posición > 3, diferencia > 10% vs más barato'
		},
		'Sin Competencia': {
			icon: '🟣',
			color: '#a855f7',
			description: 'No hay competidores registrados para este producto'
		}
	},
	
	// Helper: Interpretar Delta %
	interpretarDelta: (deltaPorc) => {
		if (deltaPorc === null || deltaPorc === undefined) return 'N/A';
		if (deltaPorc <= 0) return `${deltaPorc}% (Más barato)`;
		if (deltaPorc <= 5) return `${deltaPorc}% (Muy competitivo)`;
		if (deltaPorc <= 10) return `${deltaPorc}% (Competitivo)`;
		if (deltaPorc <= 20) return `${deltaPorc}% (Riesgo moderado)`;
		return `${deltaPorc}% (Muy caro)`;
	},
	
	// Helper: Interpretar Posición
	interpretarPosicion: (posicion, cantidadCompetidores) => {
		if (posicion === null || cantidadCompetidores === 0) return 'N/A';
		return `${posicion}º de ${cantidadCompetidores}`;
	}
};
