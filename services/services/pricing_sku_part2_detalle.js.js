// ============================================================================
// pricing_sku_part2_detalle.js
// ============================================================================
// Objetivo: Obtener la lista detallada de competidores para un SKU específico
// Sin GROUP BY: Muestra cada tienda individualmente
// ============================================================================

const query = `
DECLARE @SkuInput NVARCHAR(50) = @SkuParam;

SELECT 
    pe.tienda AS Competidor,
    pe.precio AS PrecioCompetencia,
    pe.url AS UrlCompetencia,
    pe.updated_at AS FechaActualizacion,
    pm.precio AS PrecioNuestro,
    
    -- Diferencia en pesos
    (pm.precio - pe.precio) AS DiferenciaPesos,

    -- Delta Porcentual (Vs Nosotros)
    -- Si es negativo: Ellos son más baratos. Si es positivo: Ellos son más caros.
    CASE 
        WHEN pe.precio > 0 
        THEN ROUND(((pm.precio - pe.precio) * 1.0 / pe.precio) * 100, 2)
        ELSE NULL 
    END AS DeltaPorc,

    -- Etiqueta de Estado Individual
    CASE 
        WHEN pe.precio < pm.precio THEN 'Más Barato que tú'
        WHEN pe.precio > pm.precio THEN 'Más Caro que tú'
        ELSE 'Mismo Precio'
    END AS EstadoCompetencia

FROM scraping.productosmimbral pm
INNER JOIN scraping.asociacionesproductos ap ON ap.id_producto_mimbral = pm.id
INNER JOIN scraping.productostiendasexternas pe ON pe.id = ap.id_producto_externo
WHERE pm.sku = @SkuInput
  AND pe.isActive = 1       -- Solo productos activos
  AND pe.precio > 0         -- Con precio válido
ORDER BY pe.precio ASC      -- Ordenar del más barato al más caro
`;

module.exports = {
    query,
    description: 'Obtiene detalle de competidores para un SKU'
};