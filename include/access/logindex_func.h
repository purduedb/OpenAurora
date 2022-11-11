//
// Created by pang65 on 11/1/22.
//

#ifndef DB2_PG_LOGINDEX_FUNC_H
#define DB2_PG_LOGINDEX_FUNC_H

#include "access/xlogutils.h"
#include "storage/buf_internals.h"

// BRIN
extern XLogRedoAction polar_brin_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer);
extern bool polar_brin_idx_save(XLogReaderState *record);

// GENERIC
extern bool polar_generic_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_generic_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer);

// GIN
extern bool polar_gin_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_gin_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer);

// GIST
extern bool polar_gist_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_gist_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer);

// HASH
extern bool polar_hash_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_hash_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer);

// HEAP
extern bool polar_heap_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_heap_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer);

// HEAP2
extern bool polar_heap2_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_heap2_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer);

// NBTREE
extern bool polar_btree_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_btree_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer);

// SEQUENCE
extern bool polar_seq_idx_save( XLogReaderState *record);
extern XLogRedoAction polar_seq_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer);

// XLOG
extern bool polar_xlog_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_xlog_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer);

// SPG
extern bool polar_spg_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_spg_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer);

#endif //DB2_PG_LOGINDEX_FUNC_H
