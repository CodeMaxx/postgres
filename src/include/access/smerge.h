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

#include "pg_config_manual.h"

#include "access/amapi.h"
#include "access/itup.h"
#include "access/sdir.h"
#include "access/xlogreader.h"
#include "access/attnum.h"

#include "catalog/pg_index.h"
#include "lib/stringinfo.h"
#include "storage/bufmgr.h"

#include "catalog/pg_type.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "nodes/execnodes.h"
#include "nodes/relation.h"
#include "catalog/pg_class.h"
#include "utils/rel.h"

#include "storage/smgr.h"

/*
 * Define constants here
 */
#define SMERGE_METAPAGE 0
#define MAX_K 16
#define MAX_N 8

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
extern void smergecostestimate(PlannerInfo *root, IndexPath *path, double loop_count, Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation);


typedef struct SmMetadata {
	int K;
	int N;

	int attnum;
	AttrNumber attrs[INDEX_MAX_KEYS];

	int levels[MAX_N];
	Oid tree[MAX_N][MAX_K];

	Oid curr;
	Oid root;
} SmMetadata;

typedef struct SmScanOpaqueData
{
	SmMetadata* metadata;

	Relation bt_rel;
	IndexScanDesc bt_isd;
} SmScanOpaqueData;

typedef SmScanOpaqueData* SmScanOpaque;


// btree create functions
extern Node* create_false_node(void);
extern IndexStmt* create_btree_index_stmt(Relation heap, int attsnum, AttrNumber *attrs, char *indname);

/*
 * start smerge specific
 */
extern void _sm_init_metadata(Page metapage, Oid bt_index, IndexInfo *indexInfo);
extern void _sm_writepage(Relation index, Page page, BlockNumber blkno);
extern SmMetadata* _sm_getmetadata(Relation rel);

// smerge functions
extern Relation _get_curr_btree (SmMetadata* metadata);

// smsort functions
extern void sm_flush(Relation rel, Relation heapRel, SmMetadata* metadata);
#endif   /* SMERGE_H */