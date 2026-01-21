// ============================================================================
// pricing_sku_part1_preparacion.js - VERSIÓN COMPLETA (TODO EN 1 QUERY)
// ============================================================================
// Análisis competitivo de precios: TODO en una sola ejecución
// - Filtra productos Mimbral
// - Obtiene históricos de precios
// - Calcula métricas de competencia
// - Calcula delta %, posición, status
// SALIDA: Resultado final listo para API
// ============================================================================

const query = `
-- ============================================================================
-- DECLARACIÓN DE PARÁMETROS
-- ============================================================================
DECLARE @TiendaFiltro NVARCHAR(100) = @TiendaParamInput;
DECLARE @CategoriaFiltro NVARCHAR(200) = @CategoriaParamInput;
DECLARE @CategoriaN2Filtro NVARCHAR(200) = @CategoriaN2ParamInput;
DECLARE @CategoriaN3Filtro NVARCHAR(200) = @CategoriaN3ParamInput;
DECLARE @MarcaFiltro NVARCHAR(100) = @MarcaParamInput;
DECLARE @BuscarSKUoNombre NVARCHAR(200) = @BuscarParamInput;
DECLARE @FechaInicioCustom DATETIME = @FechaInicioInput;
DECLARE @FechaFinCustom DATETIME = @FechaFinInput;
DECLARE @Periodo VARCHAR(10) = @PeriodoParamInput;

DECLARE @FechaInicio DATE, @FechaFin DATE;

-- ============================================================================
-- LÓGICA DE FECHAS
-- ============================================================================
IF @FechaFinCustom IS NULL
	SET @FechaFinCustom = CAST(GETDATE() AS DATE);
ELSE
	SET @FechaFinCustom = CAST(@FechaFinCustom AS DATE);

IF @FechaInicioCustom IS NOT NULL
	SET @FechaInicio = CAST(@FechaInicioCustom AS DATE)
ELSE
BEGIN
	SET @FechaInicio = CASE
		WHEN @Periodo = '7D' THEN DATEADD(DAY, -6, @FechaFinCustom)
		WHEN @Periodo = '30D' THEN DATEADD(DAY, -29, @FechaFinCustom)
		WHEN @Periodo = '60D' THEN DATEADD(DAY, -59, @FechaFinCustom)
		WHEN @Periodo = '90D' THEN DATEADD(DAY, -89, @FechaFinCustom)
		ELSE DATEADD(DAY, -29, @FechaFinCustom)
	END;
END;

SET @FechaFin = @FechaFinCustom;

-- ============================================================================
-- CTE: ProductosMimbral - Filtra catálogo propio
-- ============================================================================
;WITH ProductosMimbral AS (
	SELECT TOP 5  -- Límite para evitar timeout
		pm.id AS id_producto_mimbral,
		pm.tienda,
		pm.sku,
		pm.nombre,
		pm.precio AS precio_mimbral_actual,
		pm.marca,
		pm.categoria,
		pm.categoriaN2,
		pm.categoriaN3,
		pm.url,
		pm.imageurl,
		pm.marketplace
	FROM scraping.productosmimbral pm
	WHERE pm.tienda = 'MIMBRAL'
		AND (@CategoriaFiltro IS NULL OR pm.categoria LIKE '%' + @CategoriaFiltro + '%')
		AND (@CategoriaN2Filtro IS NULL OR pm.categoriaN2 LIKE '%' + @CategoriaN2Filtro + '%')
		AND (@CategoriaN3Filtro IS NULL OR pm.categoriaN3 LIKE '%' + @CategoriaN3Filtro + '%')
		AND (@MarcaFiltro IS NULL OR pm.marca LIKE '%' + @MarcaFiltro + '%')
		AND (
			@BuscarSKUoNombre IS NULL
			OR pm.sku LIKE '%' + @BuscarSKUoNombre + '%'
			OR pm.nombre LIKE '%' + @BuscarSKUoNombre + '%'
		)
	ORDER BY pm.id
),

-- ============================================================================
-- CTE: UltimoPrecioMimbral - Último precio de cada producto Mimbral
-- ============================================================================
UltimoPrecioMimbral AS (
	SELECT
		hpp.id_producto,
		hpp.precio_nuevo,
		hpp.fecha_cambio,
		ROW_NUMBER() OVER (
			PARTITION BY hpp.id_producto
			ORDER BY hpp.fecha_cambio DESC
		) AS rn
	FROM scraping.historialpreciosproducto hpp
	WHERE hpp.origen = 'mimbral'
		AND hpp.tienda = 'MIMBRAL'
		AND hpp.fecha_cambio BETWEEN @FechaInicio AND @FechaFin
),

-- ============================================================================
-- CTE: Asociacion - Mapeo Mimbral <-> Externos
-- ============================================================================
Asociacion AS (
	SELECT
		a.id_producto_mimbral,
		a.id_producto_externo
	FROM scraping.asociacionesproductos a
	WHERE a.id_producto_mimbral IS NOT NULL
		AND a.id_producto_externo IS NOT NULL
),

-- ============================================================================
-- CTE: ProductosExternos - Catálogo de competencia
-- ============================================================================
ProductosExternos AS (
	SELECT
		pe.id AS id_producto_externo,
		pe.tienda,
		pe.sku,
		pe.nombre,
		pe.precio AS precio_externo_actual,
		pe.marca,
		pe.url,
		pe.imageurl,
		pe.stock
	FROM scraping.productostiendasexternas pe
	WHERE (@TiendaFiltro IS NULL OR pe.tienda = @TiendaFiltro)
		AND ISNULL(pe.isActive, 1) = 1
),

-- ============================================================================
-- CTE: UltimoPrecioExternos - Último precio por producto externo
-- ============================================================================
UltimoPrecioExternos AS (
	SELECT
		hpp.id_producto,
		hpp.tienda,
		hpp.precio_nuevo,
		hpp.fecha_cambio,
		ROW_NUMBER() OVER(
			PARTITION BY hpp.id_producto, hpp.tienda
			ORDER BY hpp.fecha_cambio DESC
		) AS rn
	FROM scraping.historialpreciosproducto hpp
	WHERE hpp.origen = 'externo'
		AND hpp.fecha_cambio BETWEEN @FechaInicio AND @FechaFin
),

-- ============================================================================
-- CTE: RawData - Todas las combinaciones Mimbral x Competidor
-- ============================================================================
RawData AS (
	SELECT
		pm.id_producto_mimbral,
		pm.sku AS sku_mimbral,
		pm.nombre AS nombre_mimbral,
		pm.marca AS marca_mimbral,
		pm.categoria,
		pm.categoriaN2,
		pm.categoriaN3,
		pm.url AS url_mimbral,
		pm.imageurl AS imageurl_mimbral,
		pm.marketplace AS marketplace_mimbral,
		
		ISNULL(um.precio_nuevo, pm.precio_mimbral_actual) AS precio_nuestro,
		um.fecha_cambio AS fecha_precio_nuestro,
		
		pe.tienda,
		ISNULL(ue.precio_nuevo, pe.precio_externo_actual) AS precio_competidor
	FROM ProductosMimbral pm
	LEFT JOIN UltimoPrecioMimbral um
		ON um.id_producto = pm.id_producto_mimbral AND um.rn = 1
	LEFT JOIN Asociacion a
		ON a.id_producto_mimbral = pm.id_producto_mimbral
	LEFT JOIN ProductosExternos pe
		ON pe.id_producto_externo = a.id_producto_externo
	LEFT JOIN UltimoPrecioExternos ue
		ON ue.id_producto = pe.id_producto_externo AND ue.rn = 1
),

-- ============================================================================
-- CTE: ConteoCompetidores - Pre-calcula cantidad de competidores únicos
-- ============================================================================
ConteoCompetidores AS (
	SELECT
		r.id_producto_mimbral,
		COUNT(DISTINCT r.tienda) AS cantidad_competidores
	FROM RawData r
	WHERE r.precio_competidor IS NOT NULL 
		AND r.precio_competidor > 0
	GROUP BY r.id_producto_mimbral
),

-- ============================================================================
-- CTE: MetricasAgregadas - Min/Max/Competidores por producto
-- ============================================================================
MetricasAgregadas AS (
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
		
		-- Mínimo/Máximo precio competencia
		MIN(NULLIF(r.precio_competidor, 0)) 
			OVER(PARTITION BY r.id_producto_mimbral) AS precio_min_competidor,
		MAX(NULLIF(r.precio_competidor, 0)) 
			OVER(PARTITION BY r.id_producto_mimbral) AS precio_max_competidor,
		
		-- Cantidad de competidores (desde CTE pre-calculado)
		ISNULL(cc.cantidad_competidores, 0) AS cantidad_competidores,
		
		-- Cuántos competidores son más baratos
		SUM(CASE WHEN r.precio_competidor > 0 
			AND r.precio_competidor < r.precio_nuestro THEN 1 ELSE 0 END)
			OVER(PARTITION BY r.id_producto_mimbral) AS cantidad_mas_baratos,
		
		-- Tienda más barata
		MIN(CASE WHEN r.precio_competidor IS NOT NULL AND r.precio_competidor > 0
			THEN r.tienda ELSE NULL END)
			OVER(
				PARTITION BY r.id_producto_mimbral
				ORDER BY r.precio_competidor ASC
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
			) AS tienda_mas_barata
		
	FROM RawData r
	LEFT JOIN ConteoCompetidores cc
		ON cc.id_producto_mimbral = r.id_producto_mimbral
),

-- ============================================================================
-- CTE: ProductosUnicos - Deduplica a 1 fila por producto
-- ============================================================================
ProductosUnicos AS (
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
		tienda_mas_barata
	FROM MetricasAgregadas
),

-- ============================================================================
-- CTE: ResultadoFinal - Calcula Delta %, Posición y Status
-- ============================================================================
ResultadoFinal AS (
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
		precio_min_competidor,
		precio_max_competidor,
		fecha_precio_nuestro,
		
		-- ============================================================
		-- DELTA %: Diferencia porcentual vs más barato
		-- ============================================================
		CASE
			WHEN precio_min_competidor IS NULL 
				OR precio_min_competidor = 0
				OR cantidad_competidores = 0 THEN NULL
			ELSE ROUND(((precio_nuestro - precio_min_competidor) * 1.0 / precio_min_competidor) * 100, 2)
		END AS delta_vs_barato_porc,
		
		-- ============================================================
		-- POSICIÓN: Lugar en ranking (1 = más barato)
		-- ============================================================
		CASE
			WHEN cantidad_competidores = 0 THEN NULL
			ELSE 1 + ISNULL(cantidad_mas_baratos, 0)
		END AS posicion_precio,
		
		cantidad_competidores,
		cantidad_mas_baratos,
		tienda_mas_barata,
		
		-- ============================================================
		-- STATUS: Categorización inteligente
		-- ============================================================
		CASE
			WHEN cantidad_competidores = 0 THEN 'Sin Competencia'
			WHEN 1 + ISNULL(cantidad_mas_baratos, 0) = 1 THEN 'Más Barato'
			WHEN 1 + ISNULL(cantidad_mas_baratos, 0) BETWEEN 2 AND 3 THEN 'Competitivo'
			WHEN 1 + ISNULL(cantidad_mas_baratos, 0) > 3
				AND ROUND(((precio_nuestro - precio_min_competidor) / precio_min_competidor) * 100, 2) <= 10 
				THEN 'Riesgo'
			WHEN 1 + ISNULL(cantidad_mas_baratos, 0) > 3
				AND ROUND(((precio_nuestro - precio_min_competidor) / precio_min_competidor) * 100, 2) > 10 
				THEN 'Muy Caro'
			ELSE 'Sin Datos'
		END AS status,
		
		GETDATE() AS fecha_analisis
	FROM ProductosUnicos
)

-- ============================================================================
-- SELECT FINAL: Salida lista para API
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
	status AS Status,
	fecha_precio_nuestro AS FechaPrecioNuestro,
	fecha_analisis AS FechaAnalisis
FROM ResultadoFinal
ORDER BY 
	CASE status
		WHEN 'Muy Caro' THEN 1
		WHEN 'Riesgo' THEN 2
		WHEN 'Competitivo' THEN 3
		WHEN 'Más Barato' THEN 4
		WHEN 'Sin Competencia' THEN 5
		ELSE 6
	END,
	delta_vs_barato_porc DESC,
	nombre_mimbral ASC
OPTION (RECOMPILE);
`;

module.exports = {
	query,
	description: 'Análisis competitivo completo en un solo query',
	parameters: [
		{ name: '@TiendaParamInput', type: 'NVARCHAR(100)', nullable: true },
		{ name: '@CategoriaParamInput', type: 'NVARCHAR(200)', nullable: true },
		{ name: '@CategoriaN2ParamInput', type: 'NVARCHAR(200)', nullable: true },
		{ name: '@CategoriaN3ParamInput', type: 'NVARCHAR(200)', nullable: true },
		{ name: '@MarcaParamInput', type: 'NVARCHAR(100)', nullable: true },
		{ name: '@BuscarParamInput', type: 'NVARCHAR(200)', nullable: true },
		{ name: '@FechaInicioInput', type: 'DATETIME', nullable: true },
		{ name: '@FechaFinInput', type: 'DATETIME', nullable: true },
		{ name: '@PeriodoParamInput', type: 'VARCHAR(10)', nullable: true }
	]
};