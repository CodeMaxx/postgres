#include "postgres.h"
#include "access/smerge.h"

/*
 * Read tuples in correct sort order from tuplesort, and load them into
 * btree leaves.
 */
static void
_sm_merge_k(BTWriteState *wstate, BTSpool **btspool, int k)
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
     * Another BTSpool for dead tuples exists. Now we have to merge
     * btspools.
     */

    /* the preparation of merge */
    for(int i = 0; i < k; i++) {
        itup[i] = tuplesort_getindextuple(btspool[i]->sortstate,
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
        loadk = 0;       /* load BTSpool next ? */
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
                break;
            }
        }

        if(count != 1) {
            j++;
            // Add for loop for finding min
            for(; j < k; j++) {
                if(is_empty[j])
                    continue;

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
        else
            loadk = j;

        /* When we see first tuple, create first index page */
        if (state == NULL)
            state = _bt_pagestate(wstate, 0);

        /* Load min tuple into btree */
        _bt_buildadd(wstate, state, itup[loadk]);
        if (should_free[loadk])
            pfree(itup);
        itup[loadk] = tuplesort_getindextuple(btspool[loadk]->sortstate,
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