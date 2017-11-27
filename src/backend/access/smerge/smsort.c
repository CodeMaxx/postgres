#include "postgres.h"
#include "access/smerge.h"
#include "access/nbtree.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "catalog/dependency.h"
#include "catalog/pg_class.h"
#include "nodes/parsenodes.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "pg_config_manual.h"
#include "executor/tuptable.h"
#include "utils/int8.h"
#include "utils/builtins.h"

/*
 * Status record for spooling/sorting phase.  (Note we may have two of
 * these due to the special requirements for uniqueness-checking with
 * dead tuples.)
 */
struct BTSpool
{
    Tuplesortstate *sortstate;  /* state data for tuplesort.c */
    Relation    heap;
    Relation    index;
    bool        isunique;
};

/*
 * Status record for a btree page being built.  We have one of these
 * for each active tree level.
 *
 * The reason we need to store a copy of the minimum key is that we'll
 * need to propagate it to the parent node when this page is linked
 * into its parent.  However, if the page is not a leaf page, the first
 * entry on the page doesn't need to contain a key, so we will not have
 * stored the key itself on the page.  (You might think we could skip
 * copying the minimum key on leaf pages, but actually we must have a
 * writable copy anyway because we'll poke the page's address into it
 * before passing it up to the parent...)
 */
typedef struct BTPageState
{
    Page        btps_page;      /* workspace for page building */
    BlockNumber btps_blkno;     /* block # to write this page at */
    IndexTuple  btps_minkey;    /* copy of minimum key (first item) on page */
    OffsetNumber btps_lastoff;  /* last item offset loaded */
    uint32      btps_level;     /* tree level (0 = leaf) */
    Size        btps_full;      /* "full" if less than this much free space */
    struct BTPageState *btps_next;      /* link to parent level, if any */
} BTPageState;

/*
 * Overall status record for index writing phase.
 */
typedef struct BTWriteState
{
    Relation    heap;
    Relation    index;
    bool        btws_use_wal;   /* dump pages to WAL? */
    BlockNumber btws_pages_alloced;     /* # pages allocated */
    BlockNumber btws_pages_written;     /* # pages written out */
    Page        btws_zeropage;  /* workspace for filling zeroes */
} BTWriteState;


static Page _bt_blnewpage(uint32 level);
static BTPageState *_bt_pagestate(BTWriteState *wstate, uint32 level);
static void _bt_slideleft(Page page);
static void _bt_sortaddtup(Page page, Size itemsize,
               IndexTuple itup, OffsetNumber itup_off);
static void _bt_buildadd(BTWriteState *wstate, BTPageState *state,
             IndexTuple itup);
static void _bt_uppershutdown(BTWriteState *wstate, BTPageState *state);


/*
 * emit a completed btree page, and release the working storage.
 */
static void
_bt_blwritepage(BTWriteState *wstate, Page page, BlockNumber blkno)
{
    /* Ensure rd_smgr is open (could have been closed by relcache flush!) */
    RelationOpenSmgr(wstate->index);

    /* XLOG stuff */
    if (wstate->btws_use_wal)
    {
        /* We use the heap NEWPAGE record type for this */
        log_newpage(&wstate->index->rd_node, MAIN_FORKNUM, blkno, page, true);
    }

    /*
     * If we have to write pages nonsequentially, fill in the space with
     * zeroes until we come back and overwrite.  This is not logically
     * necessary on standard Unix filesystems (unwritten space will read as
     * zeroes anyway), but it should help to avoid fragmentation. The dummy
     * pages aren't WAL-logged though.
     */
    while (blkno > wstate->btws_pages_written)
    {
        if (!wstate->btws_zeropage)
            wstate->btws_zeropage = (Page) palloc0(BLCKSZ);
        /* don't set checksum for all-zero page */
        smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM,
                   wstate->btws_pages_written++,
                   (char *) wstate->btws_zeropage,
                   true);
    }

    PageSetChecksumInplace(page, blkno);

    /*
     * Now write the page.  There's no need for smgr to schedule an fsync for
     * this write; we'll do it ourselves before ending the build.
     */
    if (blkno == wstate->btws_pages_written)
    {
        /* extending the file... */
        smgrextend(wstate->index->rd_smgr, MAIN_FORKNUM, blkno,
                   (char *) page, true);
        wstate->btws_pages_written++;
    }
    else
    {
        /* overwriting a block we zero-filled before */
        smgrwrite(wstate->index->rd_smgr, MAIN_FORKNUM, blkno,
                  (char *) page, true);
    }

    pfree(page);
}


/*
 * slide an array of ItemIds back one slot (from P_FIRSTKEY to
 * P_HIKEY, overwriting P_HIKEY).  we need to do this when we discover
 * that we have built an ItemId array in what has turned out to be a
 * P_RIGHTMOST page.
 */
static void
_bt_slideleft(Page page)
{
    OffsetNumber off;
    OffsetNumber maxoff;
    ItemId      previi;
    ItemId      thisii;

    if (!PageIsEmpty(page))
    {
        maxoff = PageGetMaxOffsetNumber(page);
        previi = PageGetItemId(page, P_HIKEY);
        for (off = P_FIRSTKEY; off <= maxoff; off = OffsetNumberNext(off))
        {
            thisii = PageGetItemId(page, off);
            *previi = *thisii;
            previi = thisii;
        }
        ((PageHeader) page)->pd_lower -= sizeof(ItemIdData);
    }
}


/*
 * Add an item to a page being built.
 *
 * The main difference between this routine and a bare PageAddItem call
 * is that this code knows that the leftmost data item on a non-leaf
 * btree page doesn't need to have a key.  Therefore, it strips such
 * items down to just the item header.
 *
 * This is almost like nbtinsert.c's _bt_pgaddtup(), but we can't use
 * that because it assumes that P_RIGHTMOST() will return the correct
 * answer for the page.  Here, we don't know yet if the page will be
 * rightmost.  Offset P_FIRSTKEY is always the first data key.
 */
static void
_bt_sortaddtup(Page page,
               Size itemsize,
               IndexTuple itup,
               OffsetNumber itup_off)
{
    BTPageOpaque opaque = (BTPageOpaque) PageGetSpecialPointer(page);
    IndexTupleData trunctuple;

    if (!P_ISLEAF(opaque) && itup_off == P_FIRSTKEY)
    {
        trunctuple = *itup;
        trunctuple.t_info = sizeof(IndexTupleData);
        itup = &trunctuple;
        itemsize = sizeof(IndexTupleData);
    }

    if (PageAddItem(page, (Item) itup, itemsize, itup_off,
                    false, false) == InvalidOffsetNumber)
        elog(ERROR, "failed to add item to the index page");
}


/*
 * allocate workspace for a new, clean btree page, not linked to any siblings.
 */
static Page
_bt_blnewpage(uint32 level)
{
    Page        page;
    BTPageOpaque opaque;

    page = (Page) palloc(BLCKSZ);

    /* Zero the page and set up standard page header info */
    _bt_pageinit(page, BLCKSZ);

    /* Initialize BT opaque state */
    opaque = (BTPageOpaque) PageGetSpecialPointer(page);
    opaque->btpo_prev = opaque->btpo_next = P_NONE;
    opaque->btpo.level = level;
    opaque->btpo_flags = (level > 0) ? 0 : BTP_LEAF;
    opaque->btpo_cycleid = 0;

    /* Make the P_HIKEY line pointer appear allocated */
    ((PageHeader) page)->pd_lower += sizeof(ItemIdData);

    return page;
}


/*
 * allocate and initialize a new BTPageState.  the returned structure
 * is suitable for immediate use by _bt_buildadd.
 */
static BTPageState *
_bt_pagestate(BTWriteState *wstate, uint32 level)
{
    BTPageState *state = (BTPageState *) palloc0(sizeof(BTPageState));

    /* create initial page for level */
    state->btps_page = _bt_blnewpage(level);

    /* and assign it a page position */
    state->btps_blkno = wstate->btws_pages_alloced++;

    state->btps_minkey = NULL;
    /* initialize lastoff so first item goes into P_FIRSTKEY */
    state->btps_lastoff = P_HIKEY;
    state->btps_level = level;
    /* set "full" threshold based on level.  See notes at head of file. */
    if (level > 0)
        state->btps_full = (BLCKSZ * (100 - BTREE_NONLEAF_FILLFACTOR) / 100);
    else
        state->btps_full = RelationGetTargetPageFreeSpace(wstate->index,
                                                   BTREE_DEFAULT_FILLFACTOR);
    /* no parent level, yet */
    state->btps_next = NULL;

    return state;
}

/*----------
 * Add an item to a disk page from the sort output.
 *
 * We must be careful to observe the page layout conventions of nbtsearch.c:
 * - rightmost pages start data items at P_HIKEY instead of at P_FIRSTKEY.
 * - on non-leaf pages, the key portion of the first item need not be
 *   stored, we should store only the link.
 *
 * A leaf page being built looks like:
 *
 * +----------------+---------------------------------+
 * | PageHeaderData | linp0 linp1 linp2 ...           |
 * +-----------+----+---------------------------------+
 * | ... linpN |                                      |
 * +-----------+--------------------------------------+
 * |     ^ last                                       |
 * |                                                  |
 * +-------------+------------------------------------+
 * |             | itemN ...                          |
 * +-------------+------------------+-----------------+
 * |          ... item3 item2 item1 | "special space" |
 * +--------------------------------+-----------------+
 *
 * Contrast this with the diagram in bufpage.h; note the mismatch
 * between linps and items.  This is because we reserve linp0 as a
 * placeholder for the pointer to the "high key" item; when we have
 * filled up the page, we will set linp0 to point to itemN and clear
 * linpN.  On the other hand, if we find this is the last (rightmost)
 * page, we leave the items alone and slide the linp array over.
 *
 * 'last' pointer indicates the last offset added to the page.
 *----------
 */
static void
_bt_buildadd(BTWriteState *wstate, BTPageState *state, IndexTuple itup)
{
    Page        npage;
    BlockNumber nblkno;
    OffsetNumber last_off;
    Size        pgspc;
    Size        itupsz;

    /*
     * This is a handy place to check for cancel interrupts during the btree
     * load phase of index creation.
     */
    CHECK_FOR_INTERRUPTS();

    npage = state->btps_page;
    nblkno = state->btps_blkno;
    last_off = state->btps_lastoff;

    pgspc = PageGetFreeSpace(npage);
    itupsz = IndexTupleDSize(*itup);
    itupsz = MAXALIGN(itupsz);

    /*
     * Check whether the item can fit on a btree page at all. (Eventually, we
     * ought to try to apply TOAST methods if not.) We actually need to be
     * able to fit three items on every page, so restrict any one item to 1/3
     * the per-page available space. Note that at this point, itupsz doesn't
     * include the ItemId.
     *
     * NOTE: similar code appears in _bt_insertonpg() to defend against
     * oversize items being inserted into an already-existing index. But
     * during creation of an index, we don't go through there.
     */
    if (itupsz > BTMaxItemSize(npage))
        ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("index row size %zu exceeds maximum %zu for index \"%s\"",
                   itupsz, BTMaxItemSize(npage),
                   RelationGetRelationName(wstate->index)),
        errhint("Values larger than 1/3 of a buffer page cannot be indexed.\n"
                "Consider a function index of an MD5 hash of the value, "
                "or use full text indexing."),
                 errtableconstraint(wstate->heap,
                                    RelationGetRelationName(wstate->index))));

    /*
     * Check to see if page is "full".  It's definitely full if the item won't
     * fit.  Otherwise, compare to the target freespace derived from the
     * fillfactor.  However, we must put at least two items on each page, so
     * disregard fillfactor if we don't have that many.
     */
    if (pgspc < itupsz || (pgspc < state->btps_full && last_off > P_FIRSTKEY))
    {
        /*
         * Finish off the page and write it out.
         */
        Page        opage = npage;
        BlockNumber oblkno = nblkno;
        ItemId      ii;
        ItemId      hii;
        IndexTuple  oitup;

        /* Create new page of same level */
        npage = _bt_blnewpage(state->btps_level);

        /* and assign it a page position */
        nblkno = wstate->btws_pages_alloced++;

        /*
         * We copy the last item on the page into the new page, and then
         * rearrange the old page so that the 'last item' becomes its high key
         * rather than a true data item.  There had better be at least two
         * items on the page already, else the page would be empty of useful
         * data.
         */
        Assert(last_off > P_FIRSTKEY);
        ii = PageGetItemId(opage, last_off);
        oitup = (IndexTuple) PageGetItem(opage, ii);
        _bt_sortaddtup(npage, ItemIdGetLength(ii), oitup, P_FIRSTKEY);

        /*
         * Move 'last' into the high key position on opage
         */
        hii = PageGetItemId(opage, P_HIKEY);
        *hii = *ii;
        ItemIdSetUnused(ii);    /* redundant */
        ((PageHeader) opage)->pd_lower -= sizeof(ItemIdData);

        /*
         * Link the old page into its parent, using its minimum key. If we
         * don't have a parent, we have to create one; this adds a new btree
         * level.
         */
        if (state->btps_next == NULL)
            state->btps_next = _bt_pagestate(wstate, state->btps_level + 1);

        Assert(state->btps_minkey != NULL);
        ItemPointerSet(&(state->btps_minkey->t_tid), oblkno, P_HIKEY);
        _bt_buildadd(wstate, state->btps_next, state->btps_minkey);
        pfree(state->btps_minkey);

        /*
         * Save a copy of the minimum key for the new page.  We have to copy
         * it off the old page, not the new one, in case we are not at leaf
         * level.
         */
        state->btps_minkey = CopyIndexTuple(oitup);

        /*
         * Set the sibling links for both pages.
         */
        {
            BTPageOpaque oopaque = (BTPageOpaque) PageGetSpecialPointer(opage);
            BTPageOpaque nopaque = (BTPageOpaque) PageGetSpecialPointer(npage);

            oopaque->btpo_next = nblkno;
            nopaque->btpo_prev = oblkno;
            nopaque->btpo_next = P_NONE;        /* redundant */
        }

        /*
         * Write out the old page.  We never need to touch it again, so we can
         * free the opage workspace too.
         */
        _bt_blwritepage(wstate, opage, oblkno);

        /*
         * Reset last_off to point to new page
         */
        last_off = P_FIRSTKEY;
    }

    /*
     * If the new item is the first for its page, stash a copy for later. Note
     * this will only happen for the first item on a level; on later pages,
     * the first item for a page is copied from the prior page in the code
     * above.
     */
    if (last_off == P_HIKEY)
    {
        Assert(state->btps_minkey == NULL);
        state->btps_minkey = CopyIndexTuple(itup);
    }

    /*
     * Add the new item into the current page.
     */
    last_off = OffsetNumberNext(last_off);
    _bt_sortaddtup(npage, itupsz, itup, last_off);

    state->btps_page = npage;
    state->btps_blkno = nblkno;
    state->btps_lastoff = last_off;
}

/*
 * Finish writing out the completed btree.
 */
static void
_bt_uppershutdown(BTWriteState *wstate, BTPageState *state)
{
    BTPageState *s;
    BlockNumber rootblkno = P_NONE;
    uint32      rootlevel = 0;
    Page        metapage;

    /*
     * Each iteration of this loop completes one more level of the tree.
     */
    for (s = state; s != NULL; s = s->btps_next)
    {
        BlockNumber blkno;
        BTPageOpaque opaque;

        blkno = s->btps_blkno;
        opaque = (BTPageOpaque) PageGetSpecialPointer(s->btps_page);

        /*
         * We have to link the last page on this level to somewhere.
         *
         * If we're at the top, it's the root, so attach it to the metapage.
         * Otherwise, add an entry for it to its parent using its minimum key.
         * This may cause the last page of the parent level to split, but
         * that's not a problem -- we haven't gotten to it yet.
         */
        if (s->btps_next == NULL)
        {
            opaque->btpo_flags |= BTP_ROOT;
            rootblkno = blkno;
            rootlevel = s->btps_level;
        }
        else
        {
            Assert(s->btps_minkey != NULL);
            ItemPointerSet(&(s->btps_minkey->t_tid), blkno, P_HIKEY);
            _bt_buildadd(wstate, s->btps_next, s->btps_minkey);
            pfree(s->btps_minkey);
            s->btps_minkey = NULL;
        }

        /*
         * This is the rightmost page, so the ItemId array needs to be slid
         * back one slot.  Then we can dump out the page.
         */
        _bt_slideleft(s->btps_page);
        _bt_blwritepage(wstate, s->btps_page, s->btps_blkno);
        s->btps_page = NULL;    /* writepage freed the workspace */
    }

    /*
     * As the last step in the process, construct the metapage and make it
     * point to the new root (unless we had no data at all, in which case it's
     * set to point to "P_NONE").  This changes the index to the "valid" state
     * by filling in a valid magic number in the metapage.
     */
    metapage = (Page) palloc(BLCKSZ);
    _bt_initmetapage(metapage, rootblkno, rootlevel);
    _bt_blwritepage(wstate, metapage, BTREE_METAPAGE);
}


/*
 * Read tuples in correct sort order from tuplesort, and load them into
 * btree leaves.
 */
static void
_sm_merge_k(BTWriteState *wstate, BTSpool* btspools[], int k)
{
    BTPageState *state = NULL;
    IndexTuple  itup[k];
    bool        is_empty[k];
    bool        should_free[k];
    int loadk;
    TupleDesc   tupdes = RelationGetDescr(wstate->index);
    int         i,
                keysz = RelationGetNumberOfAttributes(wstate->index);
    ScanKey     indexScanKey = NULL;
    SortSupport sortKeys;

    /*
     * Another BTSpools for dead tuples exists. Now we have to merge
     * btspools.
     */

    /* the preparation of merge */
    for(int i = 0; i < k; i++) {
        itup[i] = tuplesort_getindextuple(btspools[i]->sortstate,
                                   true, &should_free[i]);
    }

    for(int i = 0; i < k; i++) {
        is_empty[i] = false;
    }

    indexScanKey = _bt_mkscankey_nodata(wstate->index);

    /* Prepare SortSupport data for each column */
    sortKeys = (SortSupport) palloc0(keysz * sizeof(SortSupportData));

    for (i = 0; i < keysz; i++)
    {
        SortSupport sortKey = sortKeys + i;
        ScanKey     scanKey = indexScanKey + i;
        int16       strategy;

        sortKey->ssup_cxt = CurrentMemoryContext;
        sortKey->ssup_collation = scanKey->sk_collation;
        sortKey->ssup_nulls_first =
            (scanKey->sk_flags & SK_BT_NULLS_FIRST) != 0;
        sortKey->ssup_attno = scanKey->sk_attno;
        /* Abbreviation is not supported here */
        sortKey->abbreviate = false;

        AssertState(sortKey->ssup_attno != 0);

        strategy = (scanKey->sk_flags & SK_BT_DESC) != 0 ?
            BTGreaterStrategyNumber : BTLessStrategyNumber;

        PrepareSortSupportFromIndexRel(wstate->index, strategy, sortKey);
    }

    _bt_freeskey(indexScanKey);

    for (;;)
    {
        loadk = 0;       /* load BTSpools next ? */
        int count = 0;
        for(int i = 0; i < k; i++) {
            if(itup[i] == NULL) {
                is_empty[i] = true;
                count ++;
            }
        }

        if(count == k)
            break;

        IndexTuple itup_min, itup2;
        int j;
        for(j = 0; j < k; j++) {
            if(!is_empty[j])
            {
                itup_min = itup[j];
                loadk = j;
                break;
            }
        }

        if(count != k - 1) {
            j++;
            // Add for loop for finding min
            for(; j < k; j++) {
                if(is_empty[j])
                    continue;

                itup2 = itup[j];

                /* Compare two tuples */
                for (i = 1; i <= keysz; i++)
                {
                    SortSupport entry;
                    Datum       attrDatum1,
                                attrDatum2;
                    bool        isNull1,
                                isNull2;
                    int32       compare;

                    entry = sortKeys + i - 1;
                    attrDatum1 = index_getattr(itup_min, i, tupdes, &isNull1);
                    attrDatum2 = index_getattr(itup2, i, tupdes, &isNull2);

                    compare = ApplySortComparator(attrDatum1, isNull1,
                                                  attrDatum2, isNull2,
                                                  entry);
                    if (compare > 0)
                    {
                        itup_min = itup2;
                        loadk = j;
                        break;
                    }
                    else if (compare < 0)
                        break;
                }
            }
        }

        /* When we see first tuple, create first index page */
        if (state == NULL)
            state = _bt_pagestate(wstate, 0);

        /* Load min tuple into btree */
        _bt_buildadd(wstate, state, itup[loadk]);
        if (should_free[loadk])
            pfree(itup);
        itup[loadk] = tuplesort_getindextuple(btspools[loadk]->sortstate,
                                       true, &should_free[loadk]);
    }
    pfree(sortKeys);

    /* Close down final pages and write the metapage */
    _bt_uppershutdown(wstate, state);

    /*
     * If the index is WAL-logged, we must fsync it down to disk before it's
     * safe to commit the transaction.  (For a non-WAL-logged index we don't
     * care since the index will be uninteresting after a crash anyway.)
     *
     * It's obvious that we must do this when not WAL-logging the build. It's
     * less obvious that we have to do it even if we did WAL-log the index
     * pages.  The reason is that since we're building outside shared buffers,
     * a CHECKPOINT occurring during the build has no way to flush the
     * previously written data to disk (indeed it won't know the index even
     * exists).  A crash later on would replay WAL from the checkpoint,
     * therefore it wouldn't replay our earlier WAL entries. If we do not
     * fsync those pages here, they might still not be on disk when the crash
     * occurs.
     */
    if (RelationNeedsWAL(wstate->index))
    {
        RelationOpenSmgr(wstate->index);
        smgrimmedsync(wstate->index->rd_smgr, MAIN_FORKNUM);
    }
}


static Oid
_sm_merge_create_btree (Relation heap, SmMetadata* metadata) {
    IndexStmt* btreeIndStmt;
    ObjectAddress addr;

    btreeIndStmt = create_btree_index_stmt(heap, metadata->attnum, metadata->attrs, NULL);
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

    return addr.objectId;
}

static void
_sm_merge_initialise_wstate(BTWriteState* wstate, Relation heapRel, Oid mergeBtreeOid) {

    wstate->heap = heapRel;
    wstate->index = index_open(mergeBtreeOid, ExclusiveLock);

    /*
     * We need to log index creation in WAL iff WAL archiving/streaming is
     * enabled UNLESS the index isn't WAL-logged anyway.
     */
    wstate->btws_use_wal = XLogIsNeeded() && RelationNeedsWAL(wstate->index);

    /* reserve the metapage */
    wstate->btws_pages_alloced = BTREE_METAPAGE + 1;
    wstate->btws_pages_written = 0;
    wstate->btws_zeropage = NULL;    /* until needed */
}

static void
_sm_merge_delete_btree(Oid btreeOid) {

    ObjectAddress object;

    object.classId = RelationRelationId;
    object.objectId = btreeOid;
    object.objectSubId = 0;

    performDeletion(&object, DROP_CASCADE, PERFORM_DELETION_INTERNAL);
}

static void
_sm_merge_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
         ScanKey orderbys, int norderbys) {
    if (scankey && scan->numberOfKeys > 0)
        memmove(scan->keyData,
                scankey,
                scan->numberOfKeys * sizeof(ScanKeyData));

    /* Release any held pin on a heap page */
    if (BufferIsValid(scan->xs_cbuf))
    {
        ReleaseBuffer(scan->xs_cbuf);
        scan->xs_cbuf = InvalidBuffer;
    }

    scan->xs_continue_hot = false;

    scan->kill_prior_tuple = false;      /* for safety */

    btrescan(scan, scankey, nscankeys, orderbys, norderbys);
}

void
sm_flush(Relation heapRel, SmMetadata* metadata) {
    for(int i = 0; i < metadata->N - 1; i++) {
        if(metadata->levels[i] == metadata->K) {
            Snapshot currentSnapshot;
            ScanKey scankey;

            BTSpool* btspools[MAX_K];

            for(int j = 0; j < metadata->K; j++) {
                Relation indexRel = index_open(metadata->tree[i][j], ExclusiveLock);
                btspools[j] = _bt_spoolinit(heapRel, indexRel, metadata->unique, false); // Assuming heapRel is not being used

                currentSnapshot = GetActiveSnapshot();
                IndexScanDesc scan = index_beginscan(heapRel, indexRel, currentSnapshot, metadata->attnum, 0);

    //             sk_flags;       /* flags, see below */
    // AttrNumber  sk_attno;       /* table or index column number */
    // StrategyNumber sk_strategy;  operator strategy number  -- no
    // Oid         sk_subtype;     /* strategy subtype */ -- no
    // Oid         sk_collation;   /* collation to use, if needed */ -- no
    // FmgrInfo    sk_func; -- not req
                
                scankey = palloc(sizeof(ScanKeyData));
                scankey->sk_flags = 0;
                scankey->sk_attno = metadata->attrs[0];
                scankey->sk_strategy = 5;
                scankey->sk_subtype = 23;
                scankey->sk_collation = 0;
                scankey->sk_argument = (Datum) -1000;

                scankey->sk_func.fn_addr = int4true;
                scankey->sk_func.fn_oid = 52120;
                scankey->sk_func.fn_nargs = 2;
                scankey->sk_func.fn_strict = true;
                scankey->sk_func.fn_retset = false;
                scankey->sk_func.fn_stats = 2;
                scankey->sk_func.fn_extra = NULL;
                scankey->sk_func.fn_mcxt = CurrentMemoryContext;
                scankey->sk_func.fn_expr = NULL;

                // want itups
                scan->xs_want_itup = true;

                index_rescan(scan, scankey, metadata->attnum, NULL, 0);

                // TupleTableSlot* slot;
                // slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRel));

                ItemPointer tid;
                while((tid = index_getnext_tid(scan, ForwardScanDirection)) != NULL) {
                    bool isnull[INDEX_MAX_KEYS];
                    Datum values[INDEX_MAX_KEYS];

                    for(int k = 0; k < metadata->attnum; k++ ) {
                        int keycol = metadata->attrs[k];
                        Datum iDatum;
                        bool isNull;

                        iDatum = index_getattr(scan->xs_itup,
                                      keycol,
                                      scan->xs_itupdesc,
                                      &isNull);

                        // iDatum = slot_getattr(slot, keycol, &isNull);
                        values[k] = iDatum;
                        isnull[k] = isNull;
                    }
                    _bt_spool(btspools[j], &(scan->xs_itup->t_tid), values, isnull);
                }

                SmScanOpaque so = (SmScanOpaque) scan->opaque;

                index_endscan(scan);
                if (so->bt_rel != NULL) {
                    index_close(so->bt_rel, ExclusiveLock);
                    pfree(so->bt_rel);
                    so->bt_rel = NULL;
                }
                /* Release metadata */
                if (so->metadata != NULL)
                    pfree(so->metadata);

                pfree(so);


                tuplesort_performsort(btspools[j]->sortstate);
            }

            // IndexScanDesc scan = btbeginscan(, metadata->attnum, )

            Oid mergeBtreeOid = _sm_merge_create_btree(heapRel, metadata);
            BTWriteState wstate;

            /* Initialize wstate */

            _sm_merge_initialise_wstate(&wstate, heapRel, mergeBtreeOid);

            _sm_merge_k(&wstate, btspools, metadata->K);

            metadata->tree[i + 1][metadata->levels[i + 1]++] = mergeBtreeOid;

            for (int j = 0; j < metadata->K; j++) {
                _bt_spooldestroy(btspools[j]);
            }

            for(int j = 0; j < metadata->K; j++) {
                // Delete level i btrees
                // _sm_merge_delete_btree(metadata->tree[i][j]);
                metadata->tree[i][j] = InvalidOid;
            }

            metadata->levels[i] = 0;


            // Create spools!! -> SPOOLS
            // Merge i to i + 1 -> DONE
            // Make new btree and initialise wstate -> Done
            // Increase levels[i+1] -> DONE
            // Delete level i btrees -> DONE
            // Reset level i Oids -> DONE
            // Decrease levels[i] to 0 -> DONE


        }
    }

    if(metadata->levels[metadata->N - 1] == metadata->K) {
        Snapshot currentSnapshot;
        ScanKey scankey;
        BTSpool* btspools[MAX_K + 1];
        BTWriteState wstate;

        for(int j = 0; j < metadata->K + 1; j++) {
                Relation indexRel;
                if(j != metadata->K)
                    indexRel = index_open(metadata->tree[metadata->N - 1][j], ExclusiveLock);
                else if(metadata->root != InvalidOid)
                    indexRel = index_open(metadata->root, ExclusiveLock);
                else
                    continue;

                btspools[j] = _bt_spoolinit(heapRel, indexRel, metadata->unique, false); // Assuming heapRel is not being used

                currentSnapshot = GetActiveSnapshot();
                IndexScanDesc scan = index_beginscan(heapRel, indexRel, currentSnapshot, metadata->attnum, 0);

    //             sk_flags;       /* flags, see below */
    // AttrNumber  sk_attno;       /* table or index column number */
    // StrategyNumber sk_strategy;  operator strategy number  -- no
    // Oid         sk_subtype;     /* strategy subtype */ -- no
    // Oid         sk_collation;   /* collation to use, if needed */ -- no
    // FmgrInfo    sk_func; -- not req
                
                scankey = palloc(sizeof(ScanKeyData));
                scankey->sk_flags = 0;
                scankey->sk_attno = metadata->attrs[0];
                scankey->sk_strategy = 5;
                scankey->sk_subtype = 23;
                scankey->sk_collation = 0;
                scankey->sk_argument = (Datum) -1000;

                scankey->sk_func.fn_addr = int4true;
                scankey->sk_func.fn_oid = 52120;
                scankey->sk_func.fn_nargs = 2;
                scankey->sk_func.fn_strict = true;
                scankey->sk_func.fn_retset = false;
                scankey->sk_func.fn_stats = 2;
                scankey->sk_func.fn_extra = NULL;
                scankey->sk_func.fn_mcxt = CurrentMemoryContext;
                scankey->sk_func.fn_expr = NULL;

                // want itups
                scan->xs_want_itup = true;

                index_rescan(scan, scankey, metadata->attnum, NULL, 0);

                // TupleTableSlot* slot;
                // slot = MakeSingleTupleTableSlot(RelationGetDescr(heapRel));

                ItemPointer tid;
                while((tid = index_getnext_tid(scan, ForwardScanDirection)) != NULL) {
                    bool isnull[INDEX_MAX_KEYS];
                    Datum values[INDEX_MAX_KEYS];

                    for(int k = 0; k < metadata->attnum; k++ ) {
                        int keycol = metadata->attrs[k];
                        Datum iDatum;
                        bool isNull;

                        iDatum = index_getattr(scan->xs_itup,
                                      keycol,
                                      scan->xs_itupdesc,
                                      &isNull);

                        // iDatum = slot_getattr(slot, keycol, &isNull);
                        values[k] = iDatum;
                        isnull[k] = isNull;
                    }
                    _bt_spool(btspools[j], &(scan->xs_itup->t_tid), values, isnull);
                }

                SmScanOpaque so = (SmScanOpaque) scan->opaque;

                index_endscan(scan);
                if (so->bt_rel != NULL) {
                    index_close(so->bt_rel, ExclusiveLock);
                    pfree(so->bt_rel);
                    so->bt_rel = NULL;
                }
                /* Release metadata */
                if (so->metadata != NULL)
                    pfree(so->metadata);

                pfree(so);

                tuplesort_performsort(btspools[j]->sortstate);
            }

            // IndexScanDesc scan = btbeginscan(, metadata->attnum, )

            Oid mergeBtreeOid = _sm_merge_create_btree(heapRel, metadata);

            /* Initialize wstate */

            _sm_merge_initialise_wstate(&wstate, heapRel, mergeBtreeOid);

            if(metadata->root != InvalidOid)
                _sm_merge_k(&wstate, btspools, metadata->K + 1);
            else
                _sm_merge_k(&wstate, btspools, metadata->K);

            for (int j = 0; j < metadata->K; j++) {
                _bt_spooldestroy(btspools[j]);
            }

            if(metadata->root != InvalidOid)
                _bt_spooldestroy(btspools[metadata->K]);

            metadata->root = mergeBtreeOid;

            for(int j = 0; j < metadata->K; j++) {
                // Delete level i btrees
                // _sm_merge_delete_btree(metadata->tree[i][j]);
                metadata->tree[metadata->N - 1][j] = InvalidOid;
            }

            metadata->levels[metadata->N - 1] = 0;


            // Create spools!! -> SPOOLS
            // Merge i to i + 1 -> DONE
            // Make new btree and initialise wstate -> Done
            // Increase levels[i+1] -> DONE
            // Delete level i btrees -> DONE
            // Reset level i Oids -> DONE
            // Decrease levels[i] to 0 -> DONE
        // Merge level metadata->N - 1 into root relation - metadata->K + 1 way merge
    }


// TODO - write metadata
    /*
    1. If a level fills up loop till n
    2. Create new btree when merging
    3. If level N-1 fills up, merge everything with root relation
    */
}
