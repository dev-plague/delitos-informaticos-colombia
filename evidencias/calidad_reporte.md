# Reporte de calidad

- Filas curadas: **232,939**
- Registros descartados en la curaduría: **130**
- Estado: **OK**

| Validación | Crítica | Fallos | Descripción |
|---|---|---:|---|
| `conteo_mayor_cero` | sí | 0 | La zona curada tiene al menos un registro |
| `sin_nulos_columnas_clave` | sí | 0 | Sin nulos en fecha_hecho, cod_muni, conducta y cantidad |
| `sin_id_registro_repetido` | sí | 0 | id_registro único en toda la zona curada |
| `cantidad_en_rango` | no | 0 | cantidad entre 1 y 1000 |
| `fechas_en_rango` | no | 0 | fecha_hecho entre 2000-01-01 y hoy |
| `codigos_dane_validos` | no | 0 | cod_depto de 2 dígitos y cod_muni de 5 |
| `sin_conductas_vacias` | no | 0 | Toda fila tiene descripción de conducta y código 269x |
