/*-------------------------------------------------------------------------
 *
 * smerge.h
 *	  header file for postgres stepped merge access method implementation.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/smerge.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SMERGE_H
#define SMERGE_H

#include "access/amapi.h"
#include "access/itup.h"
#include "access/sdir.h"
#include "access/xlogreader.h"
#include "catalog/pg_index.h"
#include "lib/stringinfo.h"
#include "storage/bufmgr.h"

/* Storing various indexes for one smerge */
typedef struct smerge_index_list {
    Oid oid;
    // char* table_name;
    // char* database_name;
    struct smerge_index_list* next;
} smerge_index_list;


/*
 * prototypes for functions in smerge.c (external entry points for smerge)
 */
extern Datum smergehandler(PG_FUNCTION_ARGS);
extern IndexBuildResult *smergebuild(Relation heap, Relation index,
		struct IndexInfo *indexInfo);
extern void smergebuildempty(Relation index);
extern bool smergeinsert(Relation rel, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, Relation heapRel,
		 IndexUniqueCheck checkUnique);
extern IndexScanDesc smergebeginscan(Relation rel, int nkeys, int norderbys);
extern bool smergegettuple(IndexScanDesc scan, ScanDirection dir);
extern int64 smergegetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern void smergerescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		 ScanKey orderbys, int norderbys);
extern void smergeendscan(IndexScanDesc scan);
extern void smergemarkpos(IndexScanDesc scan);
extern void smergerestrpos(IndexScanDesc scan);
extern IndexBulkDeleteResult *smergebulkdelete(IndexVacuumInfo *info,
			 IndexBulkDeleteResult *stats,
			 IndexBulkDeleteCallback callback,
			 void *callback_state);
extern IndexBulkDeleteResult *smergevacuumcleanup(IndexVacuumInfo *info,
				IndexBulkDeleteResult *stats);
extern bool smergecanreturn(Relation index, int attno);
extern void smergecostestimate();

extern smerge_index_list* smerge_index_root;

#endif   /* SMERGE_H */