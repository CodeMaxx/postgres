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
create_btree_index_stmt(Relation heap, IndexInfo *indexInfo, char *indname) {
	IndexStmt* btreeIndStmt; 
	RangeVar* relation; 
	List* indexParams;
	IndexElem* indexElem;

	ListCell* head;
	ListCell* tail;

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
	indexParams->length = 1;

// {type = T_IndexElem, name = 0x555555e80688 "uid", expr = 0x0, indexcolname = 0x0, collation = 0x0, opclass = 0x0, ordering = SORTBY_DEFAULT, nulls_ordering = SORTBY_NULLS_DEFAULT}
	indexElem = (IndexElem*) palloc(sizeof(IndexElem));
	indexElem->type = T_IndexElem; 
	indexElem->name = heap->rd_att->attrs[indexInfo->ii_KeyAttrNumbers[0] - 1]->attname.data;
	indexElem->expr = NULL; 
	indexElem->indexcolname = NULL; 
	indexElem->collation = NULL; 
	indexElem->opclass = NULL; 
	indexElem->ordering = SORTBY_DEFAULT; 
	indexElem->nulls_ordering = SORTBY_NULLS_DEFAULT;

	head = (ListCell*) palloc(sizeof(ListCell));
	indexParams->head = head;
	head->data.ptr_value = (void *) indexElem;
	head->next = NULL;

	tail = (ListCell*) palloc(sizeof(ListCell));
	indexParams->tail = tail;
	tail->data.ptr_value = (void *) indexElem;
	tail->next = NULL;

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
