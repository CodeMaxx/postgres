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
#include "commands/defrem.h"


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
	amroutine->amcanmulticol = false;
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

	btreeIndStmt = create_btree_index_stmt(heap, indexInfo, NULL);
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
	
	_sm_init_metadata(metapage, bt_index);
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
	SmMetadata* sm_metadata = _sm_getmetadata(rel);
	printf("K: %d, N: %d\n", sm_metadata->K, sm_metadata->N);
	for (int i = 0; i != sm_metadata->numList; i++) {
		printf("Btree OIDs %d: %d \n", i, sm_metadata->list[i]);
	}

	RelationCloseSmgr(rel);

	// insert into sub btrees only if there any btrees
	if (sm_metadata->numList > 0) {

		Relation btreeRel = index_open(sm_metadata->list[sm_metadata->numList - 1], RowExclusiveLock);
		
		bool b = btinsert(btreeRel, values, isnull, 
				ht_ctid, heapRel, checkUnique);

		printf("btinsert returns %d\n", b);

		index_close(btreeRel, RowExclusiveLock);
	}

	return false;
}

/*
 *	smergegettuple() -- Get the next tuple in the scan.
 */
bool
smergegettuple(IndexScanDesc scan, ScanDirection dir)
{

}

/*
 *	smergebeginscan() -- start a scan on a smerge index
 */
IndexScanDesc
smergebeginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	SmMetadata* sm_metadata;

	scan = RelationGetIndexScan(rel, nkeys, norderbys);

	sm_metadata = _sm_getmetadata(rel);

	return scan;
}

/*
 *	smergerescan() -- rescan an index relation
 */
void
smergerescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		 ScanKey orderbys, int norderbys)
{

}

/*
 *	smergeendscan() -- close down a scan
 */
void
smergeendscan(IndexScanDesc scan)
{}

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
{}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
IndexBulkDeleteResult *
smergevacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{}

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

}
