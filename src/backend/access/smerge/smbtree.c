#include "postgres.h"
#include "access/smerge.h"

Node*
create_false_node(void) {
	Const* node;

	node = (Const*) palloc(sizeof(Const));
	node->xpr.type = T_Const;
	node->consttype = BOOLOID; // Boolean
	node->consttypmod = -1; 
	node->constcollid = InvalidOid; 
	node->constlen = 1; 
	node->constvalue = 0; // False
	node->constisnull = 0; 
	node->constbyval = 1; 
	node->location = -1;

	return (Node*) node;
}


IndexStmt*
create_btree_index_stmt(Relation heap, int attsnum, AttrNumber *attrs, char *indname) {
	IndexStmt* btreeIndStmt; 
	RangeVar* relation; 
	List* indexParams;
	IndexElem* indexElem;

	ListCell* head;
	ListCell* prevCell;
	ListCell* currCell;

	relation = (RangeVar*) palloc(sizeof(RangeVar));
	relation->type =T_RangeVar;
	relation->catalogname = NULL;
	relation->schemaname = NULL;
	relation->relname = RelationGetRelationName(heap);
	relation->inhOpt = INH_DEFAULT;
	relation->relpersistence = RELPERSISTENCE_PERMANENT;
	relation->alias = NULL;
	relation->location = -1;

	btreeIndStmt = (IndexStmt*) palloc(sizeof(IndexStmt));
	btreeIndStmt->type = T_IndexStmt;
	btreeIndStmt->idxname = indname;
	btreeIndStmt->relation = relation;
	btreeIndStmt->accessMethod = "btree";
	btreeIndStmt->tableSpace = NULL;

	indexParams = (List*) palloc(sizeof(List));
	indexParams->type = T_List;
	indexParams->length = attsnum;

	prevCell = NULL;
	currCell = NULL;
	for (int i = 0; i < attsnum; i++) {
// {type = T_IndexElem, name = 0x555555e80688 "uid", expr = 0x0, indexcolname = 0x0, collation = 0x0, opclass = 0x0, ordering = SORTBY_DEFAULT, nulls_ordering = SORTBY_NULLS_DEFAULT}
		indexElem = (IndexElem*) palloc(sizeof(IndexElem));
		indexElem->type = T_IndexElem; 
		indexElem->name = heap->rd_att->attrs[attrs[i] - 1]->attname.data;
		indexElem->expr = NULL; 
		indexElem->indexcolname = NULL; 
		indexElem->collation = NULL; 
		indexElem->opclass = NULL; 
		indexElem->ordering = SORTBY_DEFAULT; 
		indexElem->nulls_ordering = SORTBY_NULLS_DEFAULT;

		prevCell = currCell;
		currCell = (ListCell*) palloc(sizeof(ListCell));
		currCell->data.ptr_value = (void *) indexElem;

		if (prevCell != NULL)
			prevCell->next = currCell;

		if (i == 0)
			indexParams->head = currCell;
	}

	indexParams->tail = currCell;
	currCell->next = NULL;

	btreeIndStmt->indexParams = indexParams;
	btreeIndStmt->options = NULL;
	btreeIndStmt->whereClause = create_false_node();
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
