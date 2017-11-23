#include "postgres.h"
#include "access/smerge.h"


void
_sm_init_metadata(Page metapage, Oid bt_index, IndexInfo *indexInfo) {
	SmMetadata* sm_metadata;

	PageInit(metapage, BLCKSZ, 0);

	sm_metadata = (SmMetadata*) PageGetContents(metapage);
	sm_metadata->K = 3;
	sm_metadata->N = 3;

	sm_metadata->attnum = indexInfo->ii_NumIndexAttrs;
	for (int i = 0; i < sm_metadata->attnum; i++)
		sm_metadata->attrs[i] = indexInfo->ii_KeyAttrNumbers[i];

	for (int i = 0; i < MAX_N; i++)
		sm_metadata->levels[i] = 0;

	for (int i = 0; i < MAX_N; i++) 
		for (int j = 0; j < MAX_K; j++)
			sm_metadata->tree[i][j] = InvalidOid;

	sm_metadata->currTuples = 0;
	sm_metadata->curr = bt_index;
	sm_metadata->root = InvalidOid;
	memcpy(sm_metadata, PageGetContents(metapage), sizeof(SmMetadata));

	((PageHeader) metapage)->pd_lower =
		((char *) sm_metadata + sizeof(SmMetadata)) - (char *) metapage;

}

void
_sm_writepage(Relation index, Page page, BlockNumber blkno) {

	/* Ensure rd_smgr is open (could have been closed by relcache flush!) */
	RelationOpenSmgr(index);

	log_newpage(&index->rd_node, MAIN_FORKNUM,
				blkno, page, true);

	PageSetChecksumInplace(page, blkno);
	smgrwrite(index->rd_smgr, MAIN_FORKNUM, blkno,
			  (char *) page, true);

	pfree(page);
}

void
_sm_write_metadata(Relation index, SmMetadata* sm_metadata) {
	Page metapage;

	metapage = (Page) palloc(BLCKSZ);
	
	PageInit(metapage, BLCKSZ, 0);

	memcpy (PageGetContents(metapage), sm_metadata, sizeof(SmMetadata));

	_sm_writepage(index, metapage, SMERGE_METAPAGE);
}

SmMetadata*
_sm_getmetadata(Relation rel) 
{
	Page		metapage;
	SmMetadata* sm_metadata;

	/* Construct metapage. */
	metapage = (Page) palloc(BLCKSZ);

	RelationOpenSmgr(rel);
	smgrread(rel->rd_smgr, MAIN_FORKNUM, SMERGE_METAPAGE,
			  (char *) metapage);

	sm_metadata = (SmMetadata*) palloc(sizeof(SmMetadata));
	memcpy(sm_metadata, PageGetContents(metapage), sizeof(SmMetadata));

	pfree(metapage);

	return sm_metadata;
}

