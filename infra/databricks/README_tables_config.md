# Nota: tables_config.sql — Referencia Histórica

**Estado:** REEMPLAZADO POR `src/engine/lakehouse_initializer.py`

## Contexto

Este archivo `tables_config.sql` fue originalmente un script SQL manual que debería ejecutarse manualmente para configurar las propiedades Delta/Iceberg de las tablas.

**Desde mayo 2026**, esta responsabilidad se ha trasladado **completamente** al código de ingesta.

## Cambio Arquitectónico

### Antes (Manual)
```
Desarrollador ejecuta:
$ databricks sql < infra/databricks/tables_config.sql
```

### Ahora (Automático, Self-Healing)
```
Motor de ingesta invoca automáticamente:
src/ingestion_engine.py (Task 1)
  └─ LakehouseInitializer.initialize_all()
     ├─ _create_schemas()
     ├─ _create_silver_tables()
     ├─ _create_gold_tables()
     └─ _create_system_tables()
```

## Ventajas

✅ **Idempotente:** Múltiples ejecuciones son seguras  
✅ **Self-Healing:** Crea lo que falta automáticamente  
✅ **Sin intervención manual:** El motor es autosuficiente  
✅ **Versionado con código:** Cambios a DDL están en Git  
✅ **Integrado en flujo:** Se ejecuta como parte de Task 1  

## Referencias

- **Implementación:** `src/engine/lakehouse_initializer.py`
- **Integración:** `src/ingestion_engine.py` (llamada automática en main)
- **Documentación:** [PLAN_EJECUTABLE_VALIDACION_MOTOR_INGESTA.md](../../docs/PLAN_EJECUTABLE_VALIDACION_MOTOR_INGESTA.md#a0-arquitectura-del-inicializador-idempotente-lakehouseinitializer)

## ¿Cuándo Usar Este Archivo?

- ❌ **NO lo ejecutes manualmente** — Es automático
- ✅ Úsalo como **referencia de propiedades Delta/Iceberg** aplicadas
- ✅ Úsalo como **documentación** si necesitas entender qué configuración se aplica
- ✅ Compáralo con `lakehouse_initializer.py` si necesitas auditación

## Migración Completada

| Componente | Antes | Ahora |
|-----------|-------|-------|
| Creación de esquemas | SQL manual | Python (LakehouseInitializer) |
| Creación de tablas Silver | SQL manual | Python (LakehouseInitializer) |
| Creación de tablas Gold | SQL manual | Python (LakehouseInitializer) |
| Creación de tablas System | SQL manual | Python (LakehouseInitializer) |
| Aplicación de propiedades | SQL manual | Python (LakehouseInitializer) |
| Trigger de ejecución | Manual (desarrollador) | Automático (Task 1) |

---

**Fecha de cambio:** Mayo 16, 2026  
**Motor responsable:** `src/engine/lakehouse_initializer.LakehouseInitializer`
