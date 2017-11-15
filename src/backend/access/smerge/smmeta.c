#include "postgres.h"
#include "access/smerge.h"


void
_sm_init_metadata(Page metapage, Oid bt_index) {
	SmMetadata* sm_metadata;

	PageInit(metapage, BLCKSZ, 0);

	sm_metadata = (SmMetadata*) PageGetContents(metapage);
	sm_metadata->K = 132;
	sm_metadata->N = 35;
	sm_metadata->numList = 1;
	sm_metadata->list[0] = bt_index;

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

