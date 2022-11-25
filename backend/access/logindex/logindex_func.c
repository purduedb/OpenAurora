//
// Created by pang65 on 11/11/22.
//
#include "c.h"
#include "postgres.h"
#include "access/logindex_func.h"

bool ParseLogIndexXlog(XLogReaderState* xlogreader) {
    bool parsed = false;
    switch (xlogreader->decoded_record->xl_rmid) {
        case RM_XLOG_ID:
            parsed = polar_xlog_idx_save(xlogreader);
            break;
        case RM_HEAP2_ID:
            parsed = polar_heap2_idx_save(xlogreader);
            break;
        case RM_HEAP_ID:
            parsed = polar_heap_idx_save(xlogreader);
            break;
        case RM_BTREE_ID:
            parsed = polar_btree_idx_save(xlogreader);
            break;
        case RM_HASH_ID:
            parsed = polar_hash_idx_save(xlogreader);
            break;
        case RM_GIN_ID:
            parsed = polar_gin_idx_save(xlogreader);
            break;
        case RM_GIST_ID:
            parsed = polar_gist_idx_save(xlogreader);
            break;
        case RM_SEQ_ID:
            parsed = polar_seq_idx_save(xlogreader);
            break;
        case RM_SPGIST_ID:
            parsed = polar_spg_idx_save(xlogreader);
            break;
        case RM_BRIN_ID:
            parsed = polar_brin_idx_save(xlogreader);
            break;
        case RM_GENERIC_ID:
            parsed = polar_generic_idx_save(xlogreader);
            break;
        default:
            break;
    }
    return parsed;
}

bool GetXlogBuffTagList(XLogReaderState* xlogreader, BufferTag** buffList, int* tagNum) {
    bool parsed = false;
    switch (xlogreader->decoded_record->xl_rmid) {
        case RM_XLOG_ID:
            parsed = polar_xlog_idx_get_bufftag_list(xlogreader, buffList, tagNum);
            break;
        case RM_HEAP2_ID:
            parsed = polar_heap2_idx_get_bufftag_list(xlogreader, buffList, tagNum);
            break;
        case RM_HEAP_ID:
            parsed = polar_heap_idx_get_bufftag_list(xlogreader, buffList, tagNum);
            break;
        case RM_BTREE_ID:
            parsed = polar_btree_idx_get_bufftag_list(xlogreader, buffList, tagNum);
            break;
        case RM_HASH_ID:
            parsed = polar_hash_idx_get_bufftag_list(xlogreader, buffList, tagNum);
            break;
        case RM_GIN_ID:
            parsed = polar_gin_idx_get_bufftag_list(xlogreader, buffList, tagNum);
            break;
        case RM_GIST_ID:
            parsed = polar_gist_idx_get_bufftag_list(xlogreader, buffList, tagNum);
            break;
        case RM_SEQ_ID:
            parsed = polar_seq_idx_get_bufftag_list(xlogreader, buffList, tagNum);
            break;
        case RM_SPGIST_ID:
            parsed = polar_spg_idx_get_bufftag_list(xlogreader, buffList, tagNum);
            break;
        case RM_BRIN_ID:
            parsed = polar_brin_idx_get_bufftag_list(xlogreader, buffList, tagNum);
            break;
        case RM_GENERIC_ID:
            parsed = polar_generic_idx_get_bufftag_list(xlogreader, buffList, tagNum);
            break;
        default:
            break;
    }
    return parsed;
}

// The calling function will hold the lock of parameter $buffer
bool XlogRedoSinglePage(XLogReaderState *xlogreader, BufferTag *bufferTag, Buffer* buffer) {
    XLogRedoAction action = BLK_NOTFOUND;
    switch (xlogreader->decoded_record->xl_rmid) {
        case RM_XLOG_ID:
            action = polar_xlog_idx_redo(xlogreader, bufferTag, buffer);
            break;
        case RM_HEAP2_ID:
            action = polar_heap2_idx_redo(xlogreader, bufferTag, buffer);
            break;
        case RM_HEAP_ID:
            action = polar_heap_idx_redo(xlogreader, bufferTag, buffer);
            break;
        case RM_BTREE_ID:
            action = polar_btree_redo(xlogreader, bufferTag, buffer);
            break;
        case RM_HASH_ID:
            action = polar_hash_idx_redo(xlogreader, bufferTag, buffer);
            break;
        case RM_GIN_ID:
            action = polar_gin_idx_redo(xlogreader, bufferTag, buffer);
            break;
        case RM_GIST_ID:
            action = polar_gist_idx_redo(xlogreader, bufferTag, buffer);
            break;
        case RM_SEQ_ID:
            action = polar_seq_idx_redo(xlogreader, bufferTag, buffer);
            break;
        case RM_SPGIST_ID:
            action = polar_spg_idx_redo(xlogreader, bufferTag, buffer);
            break;
        case RM_BRIN_ID:
            action = polar_brin_idx_redo(xlogreader, bufferTag, buffer);
            break;
        case RM_GENERIC_ID:
            action = polar_generic_idx_redo(xlogreader, bufferTag, buffer);
            break;
        default:
            // action = BLOCK NOT FOUND
            break;
    }

    if(action == BLK_NEEDS_REDO)
        return true;
    else
        return false;
}