/* SQLITE Loadable extension for reading an avro file */
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <stdbool.h>
#include <stddef.h>

#include <avro.h>

#define UNUSED(x) (void)(x)


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
    UNUSED(user_data);
    UNUSED(osize);

    if (nsize == 0) {
        sqlite3_free(ptr);
        return NULL;
    }

    return sqlite3_realloc64(ptr, nsize);
}

static void avroSetCustomAllocatorSqlite()
{
    avro_set_allocator(avroSqliteAllocator, NULL);
}

/* sqlite doesn't provide these, so we do */
static void *sqlite3_calloc(int size)
{
    void *p = sqlite3_malloc(size);
    if (p) {
        memset(p, 0, size);
    }
    return p;
}

static void *sqlite3_calloc64(sqlite3_uint64 size)
{
    void *p = sqlite3_malloc64(size);
    if (p) {
        memset(p, 0, size);
    }
    return p;
}

static int avroTypeToSqliteType(avro_schema_t schema, const char **sqliteType, char **pzErr)
{
    switch (avro_typeof(schema)) {
        // really these should be "not null"
        case AVRO_STRING:
        case AVRO_ENUM:
            *sqliteType = "text";
            break;
        case AVRO_BYTES:
            *sqliteType = "blob";
            break;
        case AVRO_INT32:
        case AVRO_INT64:
            *sqliteType = "int";
            break;
        case AVRO_FLOAT:
        case AVRO_DOUBLE:
            *sqliteType = "real";
            break;
        case AVRO_BOOLEAN:
            *sqliteType = "boolean";
            break;
        // case AVRO_NULL:
        // case AVRO_RECORD:
        // case AVRO_FIXED:
        // case AVRO_MAP:
        // case AVRO_ARRAY:
        // case AVRO_UNION:
        default:
            *pzErr = sqlite3_mprintf("Unsupported Avro type: %s", avro_schema_type_name(schema));
            return SQLITE_ERROR;
    }

    return SQLITE_OK;
}

typedef struct AvroTable {
    sqlite3_vtab base;
    char *zDb;
    char *zName;
    char *zFile;
    avro_schema_t schema;
    avro_value_iface_t *valueIface;
    avro_file_reader_t reader;
    avro_value_t value;
    bool eof;
} AvroTable;

typedef struct AvroCursor {
    sqlite3_vtab_cursor base;
    int64_t row;
} AvroCursor;

void avroTableFree(AvroTable *pAvro)
{
    avro_value_iface_decref(pAvro->valueIface);
    avro_schema_decref(pAvro->schema);
    avro_file_reader_close(pAvro->reader);
    free(pAvro);
}

int avroCreate(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr)
{
    UNUSED(pAux);

    int rc = SQLITE_OK;

    if (argc < 4) {
        *pzErr = sqlite3_mprintf("No Avro file specified");
        return SQLITE_ERROR;
    }

    size_t nDb = strlen(argv[1]);
    size_t nName = strlen(argv[2]);
    size_t nFile = strlen(argv[3]);

    size_t totalSize = sizeof(AvroTable) + nDb + nName + nFile + 3;

    AvroTable *pAvro = sqlite3_calloc64(totalSize);
    if (!pAvro) {
        *pzErr = sqlite3_mprintf("Out of memory");
        return SQLITE_NOMEM;
    }

    pAvro->base.pModule = &avroModule;
    pAvro->zDb = (char *)&pAvro[1];
    pAvro->zName = &pAvro->zDb[nDb + 1];
    pAvro->zFile = &pAvro->zName[nName + 1];
    memcpy(pAvro->zDb, argv[1], nDb);
    memcpy(pAvro->zName, argv[2], nName);

    if (argv[3][0] == '\'') {
        memcpy(pAvro->zFile, argv[3] + 1, nFile - 2);
        pAvro->zFile[nFile-2] = '\0';
    } else {
        memcpy(pAvro->zFile, argv[3], nFile);
    }

    if (avro_file_reader(pAvro->zFile, &pAvro->reader)) {
        sqlite3_free(pAvro);
        *pzErr = sqlite3_mprintf("Failed to read Avro file: %s", avro_strerror());
        return SQLITE_ERROR;
    }

    pAvro->schema = avro_file_reader_get_writer_schema(pAvro->reader);
    pAvro->valueIface = avro_generic_class_from_schema(pAvro->schema);

    int nFields = avro_schema_record_size(pAvro->schema);
    if (nFields <= 0) {
        *pzErr = sqlite3_mprintf("Avro schema has no fields");
        avroTableFree(pAvro);
        return SQLITE_ERROR;
    }

    char *createTable = sqlite3_mprintf("CREATE TABLE x(");
    for (int i = 0; createTable && i < nFields; i++) {
        const char *fieldName = avro_schema_record_field_name(pAvro->schema, i);
        avro_schema_t fieldSchema = avro_schema_record_field_get_by_index(pAvro->schema, i);

        const char *sqliteType = NULL;
        rc = avroTypeToSqliteType(fieldSchema, &sqliteType, pzErr);
        if (rc != SQLITE_OK) {
            // error already set
            sqlite3_free(createTable);
            avroTableFree(pAvro);
            return SQLITE_ERROR;
        }

        avro_schema_decref(fieldSchema);

        const char *tail = (i+1 < nFields) ? ", " : ");";
        char *createTableNext = sqlite3_mprintf("%s %s %s %s", createTable, fieldName, sqliteType, tail);
        sqlite3_free(createTable);
        createTable = createTableNext;
    }

    if (!createTable) {
        *pzErr = sqlite3_mprintf("Out of memory");
        avroTableFree(pAvro);
        return SQLITE_NOMEM;
    }

    rc = sqlite3_declare_vtab(db, createTable);
    sqlite3_free(createTable);
    if (rc != SQLITE_OK) {
        *pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
        avroTableFree(pAvro);
        return SQLITE_ERROR;
    }

    *ppVtab = (sqlite3_vtab *)pAvro;
    *pzErr = NULL;
    return rc;
}

int avroConnect(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVtab, char **pzErr)
{
    return avroCreate(db, pAux, argc, argv, ppVtab, pzErr);
}

int avroDisconnect(sqlite3_vtab *pVtab)
{
    // TODO
    UNUSED(pVtab);
    return SQLITE_OK;
}

int avroDestroy(sqlite3_vtab *pVtab)
{
    return avroDisconnect(pVtab);
}

int avroBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *info)
{
    /* Skipping for now */
    UNUSED(pVtab);
    UNUSED(info);

    return SQLITE_OK;
}

static void avroSetError(AvroTable *pAvro, const char *fmt, ...)
{
    if (pAvro->base.zErrMsg) {
        sqlite3_free(pAvro->base.zErrMsg);
    }

    va_list ap;
    va_start(ap, fmt);
    pAvro->base.zErrMsg = sqlite3_vmprintf(fmt, ap);
    va_end(ap);
}

int avroOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor)
{
    AvroTable *pAvro = (AvroTable *)pVtab;

    AvroCursor *pCur = sqlite3_calloc(sizeof(AvroCursor));
    if (!pCur) {
        avroSetError(pAvro, "Out of memory");
        return SQLITE_NOMEM;
    }
    pCur->base.pVtab = pVtab;

    *ppCursor = (sqlite3_vtab_cursor *)pCur;

    return SQLITE_OK;
}

int avroClose(sqlite3_vtab_cursor *pVtabCursor)
{
    AvroCursor *pCur = (AvroCursor *)pVtabCursor;
    sqlite3_free(pCur);
    return SQLITE_OK;
}

int avroNext(sqlite3_vtab_cursor *pVtabCursor)
{
    AvroTable *pAvro = (AvroTable *)pVtabCursor->pVtab;
    AvroCursor *pCur = (AvroCursor *)pVtabCursor;

    if (pAvro->eof) {
        avroSetError(pAvro, "Can't advance to next, already at end of file.");
        return SQLITE_ERROR;
    }

    // avro_value_reset(&pAvro->value);
    avro_generic_value_new(pAvro->valueIface, &pAvro->value);
    pCur->row++;

    // advance to the next row
    if (avro_file_reader_read_value(pAvro->reader, &pAvro->value)) {
        pAvro->eof = true;
        return SQLITE_OK;
    }

    return SQLITE_OK;
}

int avroFilter(sqlite3_vtab_cursor *pVtabCursor, int idxNum, const char *idxStr, int argc, sqlite3_value **argv)
{
    AvroTable *pAvro = (AvroTable *)pVtabCursor->pVtab;
    AvroCursor *pCur = (AvroCursor *)pVtabCursor;

    // TODO
    UNUSED(pCur);
    UNUSED(idxNum);
    UNUSED(idxStr);
    UNUSED(argc);
    UNUSED(argv);

    // reset the file reader
    pAvro->eof = false;
    avro_file_reader_close(pAvro->reader);
    avro_file_reader(pAvro->zFile, &pAvro->reader);
    avro_generic_value_new(pAvro->valueIface, &pAvro->value);

    // read the first value
    avroNext(pVtabCursor);

    return SQLITE_OK;
}

int avroEof(sqlite3_vtab_cursor *pVtabCursor)
{
    AvroTable *pAvro = (AvroTable *)pVtabCursor->pVtab;
    return pAvro->eof;
}

int avroColumn(sqlite3_vtab_cursor *pVtabCursor, sqlite3_context *ctx, int idx)
{
    AvroTable *pAvro = (AvroTable *)pVtabCursor->pVtab;
    AvroCursor *pCur = (AvroCursor *)pVtabCursor;

    // TODO
    UNUSED(pCur);
    UNUSED(ctx);
    UNUSED(idx);


    avro_schema_t fieldSchema = avro_schema_record_field_get_by_index(pAvro->schema, idx);

    avro_value_iface_t *fieldIface = avro_generic_class_from_schema(fieldSchema);
    avro_value_t fieldValue;
    avro_generic_value_new(fieldIface, &fieldValue);

    const char *name = NULL;
    if (avro_value_get_by_index(&pAvro->value, idx, &fieldValue, &name)) {
        return SQLITE_ERROR;
    }

    avro_type_t fieldType = avro_value_get_type(&fieldValue);
    switch (fieldType) {
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
            sqlite3_result_int(ctx, b); // not sure if this is correct
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
            return SQLITE_ERROR;

    }

    avro_schema_decref(fieldSchema);
    avro_value_decref(&fieldValue);

    return SQLITE_OK;
}

int avroRowid(sqlite3_vtab_cursor *pVtabCursor, sqlite_int64 *pRowid)
{
    AvroCursor *pCur = (AvroCursor *)pVtabCursor;

    *pRowid = pCur->row;

    return SQLITE_OK;
}


static sqlite3_module avroModule = {
    0,                         /* iVersion */
    avroCreate,                /* xCreate */
    avroConnect,               /* xConnect */
    avroBestIndex,             /* xBestIndex */
    avroDisconnect,            /* xDisconnect */
    avroDestroy,               /* xDestroy */
    avroOpen,                  /* xOpen - open a cursor */
    avroClose,                 /* xClose - close a cursor */
    avroFilter,                /* xFilter - configure scan constraints */
    avroNext,                  /* xNext - advance a cursor */
    avroEof,                   /* xEof - check for end of scan */
    avroColumn,                /* xColumn - read data */
    avroRowid,                 /* xRowid - read data */
    0,                         /* xUpdate */
    0,                         /* xBegin */
    0,                         /* xSync */
    0,                         /* xCommit */
    0,                         /* xRollback */
    0,                         /* xFindMethod */
    0,                         /* xRename */
    0,                         /* xSavepoint */
    0,                         /* xRelease */
    0,                         /* xRollbackTo */
    0                          /* xShadowName */
};


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_avro_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi
){
    UNUSED(pzErrMsg); // TODO

    int rc = SQLITE_OK;
    SQLITE_EXTENSION_INIT2(pApi)

    avroSetCustomAllocatorSqlite();
    rc = sqlite3_create_module(db, "avro", &avroModule, NULL);
    return rc;
}
