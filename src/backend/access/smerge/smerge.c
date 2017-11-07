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
#include "tcop/tcopprot.h"		/* pgrminclude ignore */
#include "utils/index_selfuncs.h"
#include "utils/memutils.h"

#include "access/nbtree.h"
#include "commands/defrem.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"

#define SMERGE_METAPAGE 0

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



IndexStmt*
create_btree_index_stmt(Relation heap, IndexInfo *indexInfo, const char *indname) {
	RangeVar* relation = (RangeVar*) palloc(sizeof(RangeVar));
	relation->type =T_RangeVar;
	relation->catalogname = NULL;
	relation->schemaname = NULL;
	relation->relname = RelationGetRelationName(heap);
	relation->inhOpt = INH_DEFAULT;
	relation->relpersistence = RELPERSISTENCE_PERMANENT;
	relation->alias = NULL;
	relation->location = -1;

	IndexStmt* btreeIndStmt = (IndexStmt*) palloc(sizeof(IndexStmt));
	btreeIndStmt->type = T_IndexStmt;
	btreeIndStmt->idxname = indname;
	btreeIndStmt->relation = relation;
	btreeIndStmt->accessMethod = "btree";
	btreeIndStmt->tableSpace = NULL;

	List* indexParams = (List*) palloc(sizeof(List));
	indexParams->type = T_List;
	indexParams->length = 1;

// {type = T_IndexElem, name = 0x555555e80688 "uid", expr = 0x0, indexcolname = 0x0, collation = 0x0, opclass = 0x0, ordering = SORTBY_DEFAULT, nulls_ordering = SORTBY_NULLS_DEFAULT}
	IndexElem* indexElem = (IndexElem*) palloc(sizeof(IndexElem));
	indexElem->type = T_IndexElem; 
	indexElem->name = "uid";
	indexElem->expr = NULL; 
	indexElem->indexcolname = NULL; 
	indexElem->collation = NULL; 
	indexElem->opclass = NULL; 
	indexElem->ordering = SORTBY_DEFAULT; 
	indexElem->nulls_ordering = SORTBY_NULLS_DEFAULT;

	ListCell* head = (ListCell*) palloc(sizeof(ListCell));
	indexParams->head = head;
	head->data.ptr_value = (void *) indexElem;
	head->next = NULL;

	ListCell* tail = (ListCell*) palloc(sizeof(ListCell));
	indexParams->tail = tail;
	tail->data.ptr_value = (void *) indexElem;
	tail->next = NULL;

	btreeIndStmt->indexParams = indexParams;
	btreeIndStmt->options = NULL;
	btreeIndStmt->whereClause = NULL;
	btreeIndStmt->excludeOpNames = NULL;
	btreeIndStmt->idxcomment = NULL;
	btreeIndStmt->indexOid = InvalidOid;
	btreeIndStmt->oldNode = InvalidOid;
	btreeIndStmt->unique = false;
	btreeIndStmt->primary = false;
	btreeIndStmt->isconstraint = false;
	btreeIndStmt->deferrable = false;
	btreeIndStmt->initdeferred = false;
	btreeIndStmt->transformed = true;
	btreeIndStmt->concurrent = false;
	btreeIndStmt->if_not_exists = false;
	return btreeIndStmt;
}

/*
 *	smergebuild() -- build a new btree index.
 */
IndexBuildResult *
smergebuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexStmt* btreeIndStmt = create_btree_index_stmt(heap, indexInfo, NULL);
	ObjectAddress addr = DefineIndex(RelationGetRelid(heap), 
				btreeIndStmt,
				InvalidOid,
				false,
				true,
				true,
				true);

	if ( addr.objectId == InvalidOid ) {
		printf("Error creating sub btree index\n");
	}
	else {
		printf("OID: %d \n", addr.objectId);
		smerge_index_list temp_index = smerge_index_root->next;
		while(temp_index != NULL)
	}

	IndexBuildResult* result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

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

}

/*
 *	smergegettuple() -- Get the next tuple in the scan.
 */
bool
smergegettuple(IndexScanDesc scan, ScanDirection dir)
{

}

/*
 *	smergebeginscan() -- start a scan on a btree index
 */
IndexScanDesc
smergebeginscan(Relation rel, int nkeys, int norderbys)
{}

/*
 *	smergerescan() -- rescan an index relation
 */
void
smergerescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		 ScanKey orderbys, int norderbys)
{}

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
smergecostestimate()
{

}
