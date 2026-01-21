// ============================================================================
// pricing_sku_part1_preparacion.js
// ============================================================================
// PARTE 1: Preparación de datos base
// - Filtra productos Mimbral
// - Obtiene históricos de precios
// - Realiza joins asociación + productos externos
// Retorna: RawData con TODAS las combinaciones producto Mimbral x competidor
// ============================================================================

const query = `
-- ============================================================================
-- DECLARACIÓN DE PARÁMETROS (desde el front se envían como SP parameters)
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

-- Variables internas para cálculo de fechas
DECLARE @FechaInicio DATE, @FechaFin DATE;

-- ============================================================================
-- LÓGICA DE FECHAS (igual que en top_categoria.js)
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
-- CTE: ProductosMimbral
-- Filtra catálogo propio aplicando filtros de categoría, marca, búsqueda
-- ============================================================================
;WITH ProductosMimbral AS (
	SELECT
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
	WHERE 1 = 1
		AND pm.tienda = 'MIMBRAL'
		AND (@CategoriaFiltro IS NULL OR pm.categoria LIKE '%' + @CategoriaFiltro + '%')
		AND (@CategoriaN2Filtro IS NULL OR pm.categoriaN2 LIKE '%' + @CategoriaN2Filtro + '%')
		AND (@CategoriaN3Filtro IS NULL OR pm.categoriaN3 LIKE '%' + @CategoriaN3Filtro + '%')
		AND (@MarcaFiltro IS NULL OR pm.marca LIKE '%' + @MarcaFiltro + '%')
		AND (
			@BuscarSKUoNombre IS NULL
			OR pm.sku LIKE '%' + @BuscarSKUoNombre + '%'
			OR pm.nombre LIKE '%' + @BuscarSKUoNombre + '%'
		)
),

-- ============================================================================
-- CTE: UltimoPrecioMimbral
-- Obtiene el último precio de Mimbral desde historialpreciosproductos
-- ============================================================================
UltimoPrecioMimbral AS (
	SELECT
		hpp.id_producto,
		hpp.tienda,
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
-- CTE: Asociacion
-- Mapeo de producto Mimbral con productos externos asociados
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
-- CTE: UltimoPrecioExternos
-- Último precio por producto externo y tienda
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
-- CTE: ProductosExternos
-- Catálogo de competencia con filtro de tienda
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
		pe.marketplace,
		pe.stock,
		pe.isActive
	FROM scraping.productostiendasexternas pe
	WHERE 1 = 1
		AND (@TiendaFiltro IS NULL OR pe.tienda = @TiendaFiltro)
		AND ISNULL(pe.isActive, 1) = 1
),

-- ============================================================================
-- CTE: RawData (SALIDA FINAL DE ESTA PARTE)
-- Combina: Mimbral + históricos + asociaciones + competencia
-- Resultado: 1 fila POR CADA combinación (Mimbral x Competidor)
-- ============================================================================
RawData AS (
	SELECT
		-- Datos del producto Mimbral
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
		
		-- Precio Mimbral (historial si existe, si no precio actual)
		ISNULL(um.precio_nuevo, pm.precio_mimbral_actual) AS precio_nuestro,
		um.fecha_cambio AS fecha_precio_nuestro,
		
		-- Datos del producto externo asociado
		te.id_producto_externo,
		pe.tienda,
		pe.sku AS sku_externo,
		pe.nombre AS nombre_externo,
		pe.marca AS marca_externa,
		pe.url AS url_externa,
		pe.imageurl AS imageurl_externa,
		pe.stock AS stock_competidor,
		
		-- Precio competidor (historial si existe, si no precio actual)
		ISNULL(ue.precio_nuevo, pe.precio_externo_actual) AS precio_competidor,
		ue.fecha_cambio AS fecha_precio_competidor
	FROM ProductosMimbral pm
	LEFT JOIN UltimoPrecioMimbral um
		ON um.id_producto = pm.id_producto_mimbral
		AND um.rn = 1
	LEFT JOIN Asociacion te
		ON te.id_producto_mimbral = pm.id_producto_mimbral
	LEFT JOIN ProductosExternos pe
		ON pe.id_producto_externo = te.id_producto_externo
	LEFT JOIN UltimoPrecioExternos ue
		ON ue.id_producto = pe.id_producto_externo
		AND ue.rn = 1
)

-- ============================================================================
-- SELECT: Exportar RawData para Part 2
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
	id_producto_externo,
	tienda,
	sku_externo,
	nombre_externo,
	marca_externa,
	url_externa,
	imageurl_externa,
	stock_competidor,
	precio_competidor,
	fecha_precio_competidor
FROM RawData
ORDER BY id_producto_mimbral, tienda
OPTION (RECOMPILE);
`;

module.exports = {
	query,
	description: 'PARTE 1: Preparación de datos (RawData con joins)',
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
