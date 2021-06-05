/* SQLite Loadable extension for reading an avro file */
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <avro.h>

// TODO: there are almost certainly memory errors
//       using the Avro library

static sqlite3_module avroModule;

/*
 * Allocation interface.  You can provide a custom allocator for the
 * library, should you wish.  The allocator is provided as a single
 * generic function, which can emulate the standard malloc, realloc, and
 * free functions.  The design of this allocation interface is inspired
 * by the implementation of the Lua interpreter.
 *
 * The ptr parameter will be the location of any existing memory
 * buffer.  The osize parameter will be the size of this existing
 * buffer.  If ptr is NULL, then osize will be 0.  The nsize parameter
 * will be the size of the new buffer, or 0 if the new buffer should be
 * freed.
 *
 * If nsize is 0, then the allocation function must return NULL.  If
 * nsize is not 0, then it should return NULL if the allocation fails.
 */
static void *avroSqliteAllocator(void *user_data, void *ptr, size_t osize, size_t nsize)
{
    if (nsize == 0) {
        sqlite3_free(ptr);
        return NULL;
    }

    return sqlite3_realloc64(ptr, nsize);
}

static const char *avroTypeToSqliteType(avro_schema_t schema)
{
    switch (avro_typeof(schema)) {
        case AVRO_STRING:
        case AVRO_ENUM:
            return "text";
        case AVRO_BYTES:
            return "blob";
        case AVRO_INT32:
        case AVRO_INT64:
            return "int";
        case AVRO_FLOAT:
        case AVRO_DOUBLE:
            return "real";
        case AVRO_BOOLEAN:
            return "boolean";
        // case AVRO_NULL:
        // case AVRO_RECORD:
        // case AVRO_FIXED:
        // case AVRO_MAP:
        // case AVRO_ARRAY:
        // case AVRO_UNION:
        default:
            return NULL;
    }
}

typedef struct AvroTable {
    sqlite3_vtab base;
    char *zFile;
    avro_schema_t schema;
    avro_value_iface_t *iface;
} AvroTable;

static void avroTableSetError(AvroTable *pAvro, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);

    sqlite3_free(pAvro->base.zErrMsg);
    pAvro->base.zErrMsg = sqlite3_vmprintf(fmt, ap);

    va_end(ap);
}

static int avroDisconnect(sqlite3_vtab *pVtab)
{
    AvroTable *pAvro = (AvroTable *)pVtab;

    if (pAvro) {
        avro_value_iface_decref(pAvro->iface);
        avro_schema_decref(pAvro->schema);
        sqlite3_free(pAvro->zFile);
        sqlite3_free(pAvro);
    }

    return SQLITE_OK;
}

static int avroCreate(sqlite3 *db, void *pAux, int argc, const char *const *argv, sqlite3_vtab **ppVtab, char **pzErr)
{
    int rc = SQLITE_OK;
    AvroTable *pAvro = NULL;
    char *errMsg = NULL;
    char *createTable = NULL;
    avro_file_reader_t reader = NULL;

    if (argc < 4) {
        errMsg = sqlite3_mprintf("No Avro file specified");
        rc = SQLITE_ERROR;
        goto error;
    }

    pAvro = sqlite3_malloc(sizeof(*pAvro));
    if (!pAvro) {
        errMsg = sqlite3_mprintf("Out of memory");
        rc = SQLITE_NOMEM;
        goto error;
    }
    memset(pAvro, 0, sizeof(*pAvro));

    pAvro->base.pModule = &avroModule;

    // set filename
    size_t nFile = strlen(argv[3]);
    pAvro->zFile = sqlite3_malloc64(nFile + 1);
    if (!pAvro->zFile) {
        errMsg = sqlite3_mprintf("Out of memory");
        rc = SQLITE_NOMEM;
        goto error;
    }
    // trim quotes
    if (argv[3][0] == '\'') {
        memcpy(pAvro->zFile, argv[3] + 1, nFile - 2);
        pAvro->zFile[nFile - 2] = '\0';
    } else {
        memcpy(pAvro->zFile, argv[3], nFile);
    }

    if (avro_file_reader(pAvro->zFile, &reader)) {
        errMsg = sqlite3_mprintf("Failed to read Avro file: %s", avro_strerror());
        rc = SQLITE_ERROR;
        goto error;
    }

    pAvro->schema = avro_file_reader_get_writer_schema(reader);
    pAvro->iface = avro_generic_class_from_schema(pAvro->schema);

    int nFields = avro_schema_record_size(pAvro->schema);
    if (nFields <= 0) {
        errMsg = sqlite3_mprintf("Avro schema has no fields");
        rc = SQLITE_ERROR;
        goto error;
    }

    createTable = sqlite3_mprintf("CREATE TABLE x(");
    for (int i = 0; createTable && i < nFields; i++) {
        const char *fieldName = avro_schema_record_field_name(pAvro->schema, i);
        avro_schema_t fieldSchema = avro_schema_record_field_get_by_index(pAvro->schema, i);

        const char *sqliteType = avroTypeToSqliteType(fieldSchema);
        if (!sqliteType) {
            errMsg = sqlite3_mprintf("Unsupported Avro type for field '%s'", fieldName);
            rc = SQLITE_ERROR;
            goto error;
        }

        const char *tail = (i + 1 < nFields) ? ", " : ");";
        char *createTableNext = sqlite3_mprintf("%s %s %s %s", createTable, fieldName, sqliteType, tail);
        sqlite3_free(createTable);
        createTable = createTableNext;
    }

    if (!createTable) {
        errMsg = sqlite3_mprintf("Out of memory");
        rc = SQLITE_NOMEM;
        goto error;
    }

    rc = sqlite3_declare_vtab(db, createTable);
    if (rc != SQLITE_OK) {
        errMsg = sqlite3_mprintf(sqlite3_errmsg(db));
        // rc already set
        goto error;
    }

    goto end;
error:
    // cleanup resources that may have been allocated
    avroDisconnect(&pAvro->base);
    pAvro = NULL;

end:
    // cleanup local resources
    sqlite3_free(createTable);
    avro_file_reader_close(reader);

    // set out parameters and return
    *ppVtab = (sqlite3_vtab *)pAvro;
    *pzErr = errMsg;
    return rc;
}

static int avroConnect(sqlite3 *db, void *pAux, int argc, const char *const *argv, sqlite3_vtab **ppVtab, char **pzErr)
{
    return avroCreate(db, pAux, argc, argv, ppVtab, pzErr);
}

/*
 * Only full file-scan performed.
 */
static int avroBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *info)
{
    info->estimatedCost = 1000000;

    return SQLITE_OK;
}

typedef struct AvroCursor {
    sqlite3_vtab_cursor base;
    avro_file_reader_t reader;
    avro_value_t value;
    int64_t row;
    int eof;
} AvroCursor;

static int avroOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor)
{
    AvroTable *pAvro = (AvroTable *)pVtab;

    AvroCursor *pCur = sqlite3_malloc(sizeof(*pCur));
    if (!pCur) {
        avroTableSetError(pAvro, "Out of memory");
        return SQLITE_NOMEM;
    }
    memset(pCur, 0, sizeof(*pCur));
    pCur->base.pVtab = pVtab;

    if (avro_file_reader(pAvro->zFile, &pCur->reader)) {
        sqlite3_free(pCur);
        avroTableSetError(pAvro, "Failed to open Avro file '%s' for reading.", pAvro->zFile);
        return SQLITE_ERROR;
    }
    avro_generic_value_new(pAvro->iface, &pCur->value);
    pCur->row = 0;
    pCur->eof = 0;

    *ppCursor = (sqlite3_vtab_cursor *)pCur;

    return SQLITE_OK;
}

static int avroClose(sqlite3_vtab_cursor *pVtabCursor)
{
    AvroCursor *pCur = (AvroCursor *)pVtabCursor;

    if (pCur) {
        avro_file_reader_close(pCur->reader);
        avro_value_decref(&pCur->value);
        sqlite3_free(pCur);
    }

    return SQLITE_OK;
}

static int avroNext(sqlite3_vtab_cursor *pVtabCursor)
{
    AvroTable *pAvro = (AvroTable *)pVtabCursor->pVtab;
    AvroCursor *pCur = (AvroCursor *)pVtabCursor;

    if (pCur->eof) {
        avroTableSetError(pAvro, "Can't advance to next, already at end of file.");
        return SQLITE_ERROR;
    }

    // TODO: not using api correctly or a bug, just creating a new value every time
    // avro_value_decref(&pCur->value);
    // avro_value_reset(&pCur->value);
    avro_generic_value_new(pAvro->iface, &pCur->value);
    pCur->row++;

    // advance to the next row
    if (avro_file_reader_read_value(pCur->reader, &pCur->value)) {
        pCur->eof = 1;
    }

    return SQLITE_OK;
}

static int avroFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum, const char *idxStr, int argc, sqlite3_value **argv)
{
    AvroTable *pAvro = (AvroTable *)pVtabCursor->pVtab;
    AvroCursor *pCur = (AvroCursor *)pVtabCursor;

    avro_file_reader_close(pCur->reader);
    avro_value_decref(&pCur->value);

    if (avro_file_reader(pAvro->zFile, &pCur->reader)) {
        avroTableSetError(pAvro, "Failed to open Avro file '%s'", pAvro->zFile);
        return SQLITE_ERROR;
    }
    if (avro_generic_value_new(pAvro->iface, &pCur->value)) {
        avroTableSetError(pAvro, "Failed to allocate new generic value");
        return SQLITE_ERROR;
    }
    pCur->row = 0;
    pCur->eof = 0;

    // read the first value
    avroNext(pVtabCursor);

    return SQLITE_OK;
}

static int avroEof(sqlite3_vtab_cursor *pVtabCursor)
{
    AvroCursor *pCur = (AvroCursor *)pVtabCursor;

    return pCur->eof;
}

static int avroColumn(sqlite3_vtab_cursor *pVtabCursor, sqlite3_context *ctx, int idx)
{
    int rc = SQLITE_OK;
    AvroTable *pAvro = (AvroTable *)pVtabCursor->pVtab;
    AvroCursor *pCur = (AvroCursor *)pVtabCursor;

    avro_schema_t fieldSchema = avro_schema_record_field_get_by_index(pAvro->schema, idx);
    avro_value_iface_t *fieldIface = avro_generic_class_from_schema(fieldSchema);

    avro_value_t fieldValue;
    const char *fieldName = NULL;
    if (avro_generic_value_new(fieldIface, &fieldValue)) {
        avroTableSetError(pAvro, "Failed to allocate generic value");
        rc = SQLITE_ERROR;
        goto error;
    }
    if (avro_value_get_by_index(&pCur->value, idx, &fieldValue, &fieldName)) {
        avroTableSetError(pAvro, "Failed to get field value from row");
        rc = SQLITE_ERROR;
        goto error;
    }

    switch (avro_value_get_type(&fieldValue)) {
        case AVRO_STRING: {
            const char *str = NULL;
            size_t strSize = 0;
            avro_value_get_string(&fieldValue, &str, &strSize);
            sqlite3_result_text(ctx, str, strSize, SQLITE_TRANSIENT);
            break;
        }
        case AVRO_ENUM: {
            int enumIdx = -1;
            avro_value_get_enum(&fieldValue, &enumIdx);
            const char *enumValue = avro_schema_enum_get(fieldSchema, enumIdx);
            sqlite3_result_text(ctx, enumValue, -1, SQLITE_TRANSIENT);
            break;
        }
        case AVRO_BYTES: {
            const void *buf = NULL;
            size_t bufSize = 0;
            avro_value_get_bytes(&fieldValue, &buf, &bufSize);
            sqlite3_result_blob(ctx, buf, bufSize, SQLITE_TRANSIENT);
            break;
        }
        case AVRO_INT32: {
            int32_t i = 0;
            avro_value_get_int(&fieldValue, &i);
            sqlite3_result_int(ctx, i);
            break;
        }
        case AVRO_INT64: {
            int64_t i = 0;
            avro_value_get_long(&fieldValue, &i);
            sqlite3_result_int64(ctx, i);
            break;
        }
        case AVRO_FLOAT: {
            float f = 0.0f;
            avro_value_get_float(&fieldValue, &f);
            sqlite3_result_double(ctx, f);
            break;
        }
        case AVRO_DOUBLE: {
            double d = 0.0;
            avro_value_get_double(&fieldValue, &d);
            sqlite3_result_double(ctx, d);
            break;
        }
        case AVRO_BOOLEAN: {
            int b = 0;
            avro_value_get_boolean(&fieldValue, &b);
            sqlite3_result_int(ctx, b);
            break;
        }
        // case AVRO_NULL:
        // case AVRO_RECORD:
        // case AVRO_FIXED:
        // case AVRO_MAP:
        // case AVRO_ARRAY:
        // case AVRO_UNION:
        default:
            // unsupported type
            avroTableSetError(pAvro, "Unsupported Avro value type");
            rc = SQLITE_ERROR;
            goto error;
    }

error:
    avro_value_decref(&fieldValue);
    avro_value_iface_decref(fieldIface);

    return rc;
}

static int avroRowid(sqlite3_vtab_cursor *pVtabCursor, sqlite_int64 *pRowid)
{
    AvroCursor *pCur = (AvroCursor *)pVtabCursor;

    *pRowid = pCur->row;

    return SQLITE_OK;
}

static sqlite3_module avroModule = {
    0,              /* iVersion */
    avroCreate,     /* xCreate */
    avroConnect,    /* xConnect */
    avroBestIndex,  /* xBestIndex */
    avroDisconnect, /* xDisconnect */
    avroDisconnect, /* xDestroy */
    avroOpen,       /* xOpen - open a cursor */
    avroClose,      /* xClose - close a cursor */
    avroFilter,     /* xFilter - configure scan constraints */
    avroNext,       /* xNext - advance a cursor */
    avroEof,        /* xEof - check for end of scan */
    avroColumn,     /* xColumn - read data */
    avroRowid,      /* xRowid - read data */
    0,              /* xUpdate */
    0,              /* xBegin */
    0,              /* xSync */
    0,              /* xCommit */
    0,              /* xRollback */
    0,              /* xFindMethod */
    0,              /* xRename */
    0,              /* xSavepoint */
    0,              /* xRelease */
    0,              /* xRollbackTo */
    0               /* xShadowName */
};

int sqlite3_avro_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi)
{
    int rc = SQLITE_OK;
    SQLITE_EXTENSION_INIT2(pApi)

    *pzErrMsg = NULL;
    avro_set_allocator(avroSqliteAllocator, NULL);

    rc = sqlite3_create_module(db, "avro", &avroModule, NULL);
    return rc;
}
