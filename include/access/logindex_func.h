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
extern bool polar_brin_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum);

// GENERIC
extern bool polar_generic_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_generic_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer);
extern bool polar_generic_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum);

// GIN
extern bool polar_gin_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_gin_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer);
extern bool polar_gin_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum);


// GIST
extern bool polar_gist_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_gist_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer);
extern bool polar_gist_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum);

// HASH
extern bool polar_hash_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_hash_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer);
extern bool polar_hash_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum);

// HEAP
extern bool polar_heap_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_heap_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer);
extern bool polar_heap_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum);

// HEAP2
extern bool polar_heap2_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_heap2_idx_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer);
extern bool polar_heap2_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum);

// NBTREE
extern bool polar_btree_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_btree_redo(XLogReaderState *record, BufferTag *tag, Buffer *buffer);
extern bool polar_btree_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum);

// SEQUENCE
extern bool polar_seq_idx_save( XLogReaderState *record);
extern XLogRedoAction polar_seq_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer);
extern bool polar_seq_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum);

// XLOG
extern bool polar_xlog_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_xlog_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer);
extern bool polar_xlog_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum);

// SPG
extern bool polar_spg_idx_save(XLogReaderState *record);
extern XLogRedoAction polar_spg_idx_redo(XLogReaderState *record,  BufferTag *tag, Buffer *buffer);
extern bool polar_spg_idx_get_bufftag_list(XLogReaderState *record, BufferTag** buffertagList, int* tagNum);

// Global parse xlog get buff tag
extern bool GetXlogBuffTagList(XLogReaderState* xlogreader, BufferTag** buffList, int* tagNum);
extern bool XlogRedoSinglePage(XLogReaderState *xlogreader, BufferTag *bufferTag, Buffer* buffer);
#endif //DB2_PG_LOGINDEX_FUNC_H
