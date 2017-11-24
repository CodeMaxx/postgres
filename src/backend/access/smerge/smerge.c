/*-------------------------------------------------------------------------
 *
 * smerge.c
 *	  Implementation of Lehman and Yao's btree management algorithm for
 *	  Postgres.
 *
 * NOTES
 *	  This file contains only the public interface routines.
 *
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/smerge/smerge.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/smerge.h"
#include "access/relscan.h"
#include "access/xlog.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "storage/lockdefs.h"
#include "tcop/tcopprot.h"		/* pgrminclude ignore */
#include "utils/index_selfuncs.h"
#include "utils/memutils.h"

#include "access/nbtree.h"


#include <string.h>


/*
 * Stepped Merge handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
smergehandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = true;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = smergebuild;
	amroutine->ambuildempty = smergebuildempty;
	amroutine->aminsert = smergeinsert;
	amroutine->ambulkdelete = smergebulkdelete;
	amroutine->amvacuumcleanup = smergevacuumcleanup;
	amroutine->amcanreturn = smergecanreturn;
	amroutine->amcostestimate = smergecostestimate;
	amroutine->amoptions = NULL;
	amroutine->amproperty = NULL;
	amroutine->amvalidate = NULL;
	amroutine->ambeginscan = smergebeginscan;
	amroutine->amrescan = smergerescan;
	amroutine->amgettuple = smergegettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = smergeendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;

	PG_RETURN_POINTER(amroutine);
}


/*
 *	smergebuild() -- build a new btree index.
 */
IndexBuildResult *
smergebuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult* result;

	IndexStmt* btreeIndStmt;
	ObjectAddress addr;

	Oid bt_index;
	Page		metapage;

	btreeIndStmt = create_btree_index_stmt(heap, indexInfo->ii_NumIndexAttrs, indexInfo->ii_KeyAttrNumbers, NULL);
	addr = DefineIndex(RelationGetRelid(heap), 
						btreeIndStmt,
						InvalidOid,
						false,
						true,
						false,
						true);

	if ( addr.objectId == InvalidOid ) {
		printf("Error creating sub btree index\n");
	}
	else {
		printf("OID: %d \n", addr.objectId);
	}

	bt_index = addr.objectId;


	/* Construct metapage. */
	metapage = (Page) palloc(BLCKSZ);
	
	_sm_init_metadata(metapage, bt_index, indexInfo);
	_sm_writepage(index, metapage, SMERGE_METAPAGE);
	

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = 0;
	result->index_tuples = 0;

	return result;
}


/*
 *	smergebuildempty() -- build an empty btree index in the initialization fork
 */
void
smergebuildempty(Relation index)
{
	Page		metapage;

	/* Construct metapage. */
	metapage = (Page) palloc(BLCKSZ);

	PageInit(metapage, BLCKSZ, 0 /*for now, need to add a metadata size struct*/);
	
	PageSetChecksumInplace(metapage, SMERGE_METAPAGE);
	smgrwrite(index->rd_smgr, INIT_FORKNUM, SMERGE_METAPAGE,
			  (char *) metapage, true);
	log_newpage(&index->rd_smgr->smgr_rnode.node, INIT_FORKNUM,
				SMERGE_METAPAGE, metapage, false);
	/*
	 * An immediate sync is required even if we xlog'd the page, because the
	 * write did not go through shared_buffers and therefore a concurrent
	 * checkpoint may have moved the redo pointer past our xlog record.
	 */
	smgrimmedsync(index->rd_smgr, INIT_FORKNUM);
}

Relation
_get_curr_btree (SmMetadata* metadata) {
	return index_open(metadata->curr, RowExclusiveLock);
}

/*
 *	smergeinsert() -- insert an index tuple into a btree.
 *
 *		Descend the tree recursively, find the appropriate location for our
 *		new tuple, and put it there.
 */
bool
smergeinsert(Relation rel, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, Relation heapRel,
		 IndexUniqueCheck checkUnique)
{
	bool b;
	ObjectAddress addr;
	Relation btreeRel;
	SmMetadata* sm_metadata = _sm_getmetadata(rel);
	printf("K: %d, N: %d\n", sm_metadata->K, sm_metadata->N);
	printf("Curr BTree OID: %d\n", sm_metadata->curr);

	RelationCloseSmgr(rel);

	// insert into sub btrees only if there any btrees
	btreeRel = _get_curr_btree(sm_metadata);

	b = btinsert(btreeRel, values, isnull, 
			ht_ctid, heapRel, checkUnique);

	printf("btinsert returns %d\n", b);

	index_close(btreeRel, RowExclusiveLock);
	sm_metadata->currTuples++;

	if (sm_metadata->currTuples >= MAX_INMEM_TUPLES) {
		printf("Exceeded! (:3)\n");
		sm_metadata->tree[0][sm_metadata->levels[0]++] = sm_metadata->curr;
		addr = _sm_create_curr_btree(heapRel, sm_metadata);
		sm_metadata->currTuples = 0;
		if (addr.objectId != InvalidOid)
			sm_metadata->curr = addr.objectId;
		else 
			printf("Error in creating a sub-btree\n");

		// call merge func here
	}

	_sm_write_metadata(rel, sm_metadata);

	return false;
}

/*
 *	smergegettuple() -- Get the next tuple in the scan.
 */
bool
smergegettuple(IndexScanDesc scan, ScanDirection dir)
{
	IndexScanDesc bt_scan;
	bool res;
	Oid bt_oid;
	SmMetadata* metadata;
	SmScanOpaque so = (SmScanOpaque) scan->opaque;

	metadata = so->metadata;

	bt_scan = so->bt_isd;

	scan->xs_recheck = false;

	bt_scan->xs_cbuf = scan->xs_cbuf;

	res = btgettuple(so->bt_isd, dir);
	// printf("Outer btgettuple returns %d\n", res);

	scan->xs_ctup = bt_scan->xs_ctup;

	scan->xs_itup = bt_scan->xs_itup;
	scan->xs_itupdesc = bt_scan->xs_itupdesc;
	
	while (!res) {
//		if (so->bt_isd != NULL) {
//			btendscan(so->bt_isd);
//			pfree(so->bt_isd);
//			so->bt_isd = NULL;
//		}

		// printf("CurrentLevel: %d\n", so->currlevel);
		index_close(so->bt_rel, RowExclusiveLock);

		while ((so->currlevel == -1 || so->currpos >= metadata->levels[so->currlevel]) && so->currlevel < metadata->N) {
			so->currpos = 0;
			so->currlevel ++;
		}

		res = (so->currlevel == metadata->N);

		if (!res) {
			bt_oid = metadata->tree[so->currlevel][so->currpos];
			so->currpos ++;

			// printf("Opening Btree: %d\n", bt_oid);
			so->bt_rel = index_open(bt_oid, RowExclusiveLock);
			so->bt_isd = btbeginscan(so->bt_rel, scan->numberOfKeys, scan->numberOfOrderBys);

			bt_scan = so->bt_isd;

			bt_scan->heapRelation = scan->heapRelation;
			bt_scan->xs_snapshot = scan->xs_snapshot;

			/* Release any held pin on a heap page */
			if (BufferIsValid(bt_scan->xs_cbuf))
			{
				ReleaseBuffer(bt_scan->xs_cbuf);
				bt_scan->xs_cbuf = InvalidBuffer;
			}

			bt_scan->xs_continue_hot = false;

			bt_scan->kill_prior_tuple = false;		/* for safety */
			

			btrescan(so->bt_isd, scan->keyData, scan->numberOfKeys, scan->orderByData, scan->numberOfOrderBys);

			bt_scan->xs_cbuf = scan->xs_cbuf;

			res = btgettuple(so->bt_isd, dir);
			// printf("btgettuple returns %d\n", res);

			scan->xs_ctup = bt_scan->xs_ctup;

			scan->xs_itup = bt_scan->xs_itup;
			scan->xs_itupdesc = bt_scan->xs_itupdesc;		
		}
		else {
			res = false;
			break;
		}
		// printf("Debug: btgettuple returns %d\n", res);
	} 

	/* If we're out of index entries, we're done */
	// if (!res)
	// {
	// 	/* ... but first, release any held pin on a heap page */
	// 	if (BufferIsValid(bt_scan->xs_cbuf))
	// 	{
	// 		ReleaseBuffer(bt_scan->xs_cbuf);
	// 		bt_scan->xs_cbuf = InvalidBuffer;
	// 	}
	// 	return false;
	// }

	// pgstat_count_index_tuples(bt_scan->indexRelation, 1);

	/* Return the TID of the tuple we found. */
	// return &bt_scan->xs_ctup.t_self;

	return res;
}

/*
 *	smergebeginscan() -- start a scan on a smerge index
 */
IndexScanDesc
smergebeginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	SmScanOpaque so;

	scan = RelationGetIndexScan(rel, nkeys, norderbys);
	
	// smerge metadata and stuff needed for successful scan
	so = palloc(sizeof(SmScanOpaqueData));
	so->metadata = _sm_getmetadata(rel);
	so->currlevel = -1;
	so->currpos = -1;

	so->bt_rel = _get_curr_btree(so->metadata);
	so->bt_isd = btbeginscan(so->bt_rel, nkeys, norderbys);

	scan->xs_itupdesc = RelationGetDescr(rel);

	scan->opaque = so;

	return scan;
}

/*
 *	smergerescan() -- rescan an index relation
 */
void
smergerescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		 ScanKey orderbys, int norderbys)
{
	
	IndexScanDesc bt_scan;
	SmScanOpaque so = (SmScanOpaque) scan->opaque;

	bt_scan = so->bt_isd;

	bt_scan->heapRelation = scan->heapRelation;
	bt_scan->xs_snapshot = scan->xs_snapshot;

	if (scankey && scan->numberOfKeys > 0)
		memmove(scan->keyData,
				scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));

	/* Release any held pin on a heap page */
	if (BufferIsValid(bt_scan->xs_cbuf))
	{
		ReleaseBuffer(bt_scan->xs_cbuf);
		bt_scan->xs_cbuf = InvalidBuffer;
	}

	bt_scan->xs_continue_hot = false;

	bt_scan->kill_prior_tuple = false;		/* for safety */

	btrescan(so->bt_isd, scankey, nscankeys, orderbys, norderbys);
}

/*
 *	smergeendscan() -- close down a scan
 */
void
smergeendscan(IndexScanDesc scan)
{
	SmScanOpaque so = (SmScanOpaque) scan->opaque;

	/* Release metadata */
	if (so->metadata != NULL) 
		pfree(so->metadata);

	pfree(so);
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
IndexBulkDeleteResult *
smergebulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			 IndexBulkDeleteCallback callback, void *callback_state)
{
	return NULL;
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
IndexBulkDeleteResult *
smergevacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	return NULL;
}

/*
 *	btcanreturn() -- Check whether btree indexes support index-only scans.
 *
 * btrees always do, so this is trivial.
 */
bool
smergecanreturn(Relation index, int attno)
{
	return true;
}

void
smergecostestimate(PlannerInfo *root,
                IndexPath *path,
                double loop_count,
                Cost *indexStartupCost,
                Cost *indexTotalCost,
                Selectivity *indexSelectivity,
                double *indexCorrelation)
{
	*indexStartupCost = 0.0;
	*indexTotalCost = 0.01;
	*indexSelectivity = 0.0;
	*indexCorrelation = 0.9;
}
