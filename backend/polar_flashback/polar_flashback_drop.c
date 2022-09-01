/*-------------------------------------------------------------------------
 *
 * polar_flashback_drop.c
 *
 *
 * Copyright (c) 2020-2021, Alibaba-inc PolarDB Group
 *
 * IDENTIFICATION
 *    src/backend/polar_flashback/polar_flashback_drop.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_type.h"
#include "commands/schemacmds.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "polar_flashback/polar_flashback_drop.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


#define FLSD_LENGTH 30

static void
polar_grant_recyclebin(char *trashcan)
{
	RoleSpec *spec;
	GrantStmt *grantStmt;

	spec = makeNode(RoleSpec);
	grantStmt = makeNode(GrantStmt);

	spec->roletype = ROLESPEC_PUBLIC;

	grantStmt->is_grant = true;
	grantStmt->targtype = ACL_TARGET_OBJECT;
	grantStmt->objtype = OBJECT_SCHEMA;
	grantStmt->objects = list_make1(makeString(trashcan));
	grantStmt->grantees = list_make1(spec);
	grantStmt->grant_option = false;
	grantStmt->behavior = DROP_RESTRICT;
	ExecuteGrantStmt(grantStmt);
	pfree(spec);
	pfree(grantStmt);
}

static ObjectAddresses *
polar_get_ObjectAddresses(Oid *relOid, Oid rel_oid, int len)
{
	ObjectAddresses *objects;
	ObjectAddress obj;
	int i;
	objects = new_object_addresses();

	if (len == 0)
	{
		obj.classId = RelationRelationId;
		obj.objectId = rel_oid;
		obj.objectSubId = 0;
		add_exact_object_address(&obj, objects);
	}
	else
	{
		for (i = 0; i < len; i++)
		{
			obj.classId = RelationRelationId;
			obj.objectId = relOid[i];
			obj.objectSubId = 0;
			add_exact_object_address(&obj, objects);
		}
	}

	return objects;
}

static void
polar_drop_table(Oid *relOid, Oid rel_oid, int len)
{
	ObjectAddresses *objects;
	objects = polar_get_ObjectAddresses(relOid, rel_oid, len);
	performMultipleDeletions(objects, DROP_CASCADE, 0);
	free_object_addresses(objects);
}

static void
polar_report_dependency(Oid *relOid, int len)
{
	ObjectAddresses *objects;
	objects = polar_get_ObjectAddresses(relOid, 0, len);
	polar_find_report_dependentobjects(objects);
	free_object_addresses(objects);
}

static void
polar_rename_table(RangeVar *r, char *new_table_name)
{
	RenameStmt  *newRenameStmt;
	newRenameStmt = makeNode(RenameStmt);
	newRenameStmt->type = T_RenameStmt;
	newRenameStmt->renameType = OBJECT_TABLE;
	newRenameStmt->relation = r;
	newRenameStmt->newname = new_table_name;
	RenameRelation(newRenameStmt);
	CommandCounterIncrement();
	pfree(newRenameStmt);
}

/*
 *POLAR: rename the table which will be drop to the recyclebin. 
 */
static void
polar_construct_newname(char *new_table_name, char *schema_name, char *tbl_name, Oid nspOid)
{
	char        schema_oidstr[FLSD_LENGTH];
	char        time_str[FLSD_LENGTH];
	int         newtable_len;

	TimestampTz tz = GetCurrentTransactionStartTimestamp();

	pg_lltoa(tz, time_str);
	pg_lltoa(nspOid, schema_oidstr);

	newtable_len = strlen(schema_name) + strlen(tbl_name) + strlen(time_str) + 2;

	snprintf(new_table_name, newtable_len, "%s$%s$%s", schema_name, tbl_name, time_str);
}

/*
 *POLAR: Get the table according to the order of putting it in the recyclebin.
 */
static char *
polar_get_table_according_time(char *schema_name, char *rel_name, bool flashback)
{
	IndexScanDesc   scandesc;
	HeapTuple       tuple;
	Form_pg_class   pg_class_tuple;
	int64           time;

	char            *key_1;
	char            *key_2;
	char            *relname = NULL;

	Relation        currentRelation;
	Relation        RelationDesc;
	ScanKey         scan_keys;

	scan_keys = (ScanKey) palloc(2 * sizeof(ScanKeyData));

	key_1 = psprintf("%s$%s$", schema_name, rel_name);
	key_2 = psprintf("%s$%s%c", schema_name, rel_name, '%');

	ScanKeyEntryInitialize(&scan_keys[0], 0, 1, BTGreaterEqualStrategyNumber,
						   NAMEOID, 0, F_NAMEGE, CStringGetDatum(key_1));
	ScanKeyEntryInitialize(&scan_keys[1], 0, 1, BTLessStrategyNumber,
						   NAMEOID, 0, F_NAMELT, CStringGetDatum(key_2));

	currentRelation = heap_open(RelationRelationId, AccessShareLock);
	RelationDesc = index_open(ClassNameNspIndexId, AccessShareLock);

	scandesc = index_beginscan(currentRelation,
							   RelationDesc,
							   SnapshotSelf,//SnapshotAny
							   2,
							   0);

	index_rescan(scandesc, scan_keys, 2, NULL, 0);

	while ((tuple = index_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		int64  temp_time;
		char  *temp_relname;
		char  *int64_str;
		pg_class_tuple = (Form_pg_class) GETSTRUCT(tuple);

		temp_relname = pg_class_tuple->relname.data;
		int64_str = strrchr(temp_relname, '$') + 1;

		if ((pg_class_tuple->relowner != GetUserId() && !superuser() && !polar_superuser()) || !int64_str || int64_str[0] < '0' || int64_str[0] > '9')
			continue;

		temp_time = strtol(int64_str, NULL, 10);

		if (!relname || (flashback && timestamp_cmp_internal(time, temp_time) == -1) ||
				(!flashback && timestamp_cmp_internal(time, temp_time) == 1))
		{
			relname = temp_relname;
			time = temp_time;
		}

	}

	index_endscan(scandesc);

	heap_close(currentRelation, AccessShareLock);
	index_close(RelationDesc, AccessShareLock);

	pfree(scan_keys);
	pfree(key_1);
	pfree(key_2);

	return relname;
}

/*
 *POLAR: Delete the table in the recyclebin
 */
static void
polar_purge_table(DropStmt *stmt, char *trashcan_nspname)
{
	ListCell    *cell;
	RangeVar    *r;
	Oid         relOid;
	Oid         nspOid;
	char        *schema_name;
	char        *new_table_name = NULL;

	if (stmt->objects->length > 1)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("purge do not support multi table")));

	cell = list_head(stmt->objects);

	r = makeRangeVarFromNameList((List *) lfirst(cell));

	if (!r->schemaname)
	{
		nspOid  = RangeVarGetAndCheckCreationNamespace(r, NoLock, NULL);
		schema_name = get_namespace_name(nspOid);
	}
	else
		schema_name = r->schemaname;

	new_table_name = polar_get_table_according_time(schema_name, r->relname, false);

	if (!new_table_name)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" do not exist in recyclebin ", r->relname)));

	nspOid = get_namespace_oid(trashcan_nspname, false);
	relOid = get_relname_relid(new_table_name, nspOid);
	polar_drop_table(NULL, relOid, 0);

}


/*
 * POLAR: Get partition table and invalid table
 */
static void
polar_find_partitionAndInvalidOid(DropStmt *stmt,
								  int *number_object_p, int *number_object,
								  bool *isInvalid, RangeVar **arr_r,
								  Oid *arr_reloid_p, Oid *arr_reloid)
{
	ListCell *cell;
	Oid         namespaceId;
	Oid         relid;
	Relation    rel;
	RangeVar    *r;

	foreach (cell, stmt->objects)
	{
		r = makeRangeVarFromNameList((List *) lfirst(cell));

		if (r->schemaname)
		{
			namespaceId = LookupExplicitNamespace(r->schemaname, stmt->missing_ok);

			if (stmt->missing_ok && !OidIsValid(namespaceId))
				relid = InvalidOid;
			else
				relid = get_relname_relid(r->relname, namespaceId);
		}
		else
			relid = RelnameGetRelid(r->relname);

		if (!OidIsValid(relid))
		{
			*isInvalid = true;
			*number_object = 0;
			*number_object_p = 0;
			return ;
		}

		rel = RelationIdGetRelation(relid);

		if (rel->rd_rel->relkind == 'p' || rel->rd_rel->relispartition)
		{
			arr_reloid_p[*number_object_p] = relid;
			(*number_object_p)++;
		}
		else
		{
			arr_reloid[*number_object] = relid;
			arr_r[*number_object] = r;
			(*number_object)++;
		}

		RelationClose(rel);
	}
}

static void
polar_create_grant_recyclebin(char *trashcan_nspname)
{
	CreateSchemaStmt    *newcreateStmt;
	newcreateStmt = makeNode(CreateSchemaStmt);
	newcreateStmt->type = T_CreateSchemaStmt;
	newcreateStmt->schemaname = trashcan_nspname;
	CreateSchemaCommand(newcreateStmt, NULL, 0, 0);
	polar_grant_recyclebin(trashcan_nspname);
	pfree(newcreateStmt);
}

static void
polar_deal_tempAndRecyclebin_table(PlannedStmt *pstmt, const char *queryString,
								   ProcessUtilityContext context,
								   ParamListInfo params, QueryEnvironment *queryEnv,
								   DestReceiver *dest, char *completionTag,
								   DropStmt *stmt, RangeVar **arr_r,
								   Oid *arr_reloid, Oid out_NspOid, int i)
{
	if (isAnyTempNamespace(out_NspOid))
		polar_drop_table(NULL, arr_reloid[i], 0);
	else
	{
		List *objects;

		if (arr_r[i]->schemaname)
		{
			objects = list_make1(lcons(makeString(arr_r[i]->schemaname),
									   list_make1(makeString(arr_r[i]->relname))));
		}
		else
			objects = list_make1(list_make1(makeString(arr_r[i]->relname)));

		stmt->objects = objects;
		pstmt->utilityStmt = (Node *)stmt ;
		standard_ProcessUtility(pstmt, queryString,
								context, params, queryEnv,
								dest, completionTag);
	}
}

static void
polar_drop_to_recyclebin(PlannedStmt *pstmt, const char *queryString,
						 ProcessUtilityContext context,
						 ParamListInfo params, QueryEnvironment *queryEnv,
						 DestReceiver *dest, char *completionTag,
						 DropStmt *stmt, RangeVar **arr_r,
						 char *schema_name, Oid out_NspOid, int i)
{
	RangeVar    *r;
	int         new_maxlen = strlen(schema_name) + strlen(arr_r[i]->relname) + FLSD_LENGTH + 2;
	char        new_table_name[new_maxlen] ;
	AlterObjectSchemaStmt *newstmt = makeNode(AlterObjectSchemaStmt);

	memset(new_table_name, '\0', new_maxlen);

	polar_construct_newname(new_table_name, schema_name, arr_r[i]->relname, out_NspOid);

	newstmt->objectType = stmt->removeType;
	newstmt->newschema = RECYCLEBINNAME;
	newstmt->missing_ok = stmt->missing_ok;

	/*If there is no recyclebin schema, create the recyclebin schema*/
	if (!SearchSysCacheExists1(NAMESPACENAME, PointerGetDatum(RECYCLEBINNAME)))
		polar_create_grant_recyclebin(RECYCLEBINNAME);

	polar_rename_table(arr_r[i], new_table_name);

	r = makeRangeVar(schema_name, new_table_name, -1);
	newstmt->relation = r;
	pstmt->utilityStmt = (Node *) newstmt;

	standard_ProcessUtility(pstmt, queryString,
							context, params, queryEnv,
							dest, completionTag);
	pfree(newstmt);
}

/*
 *POLAR: drop table, put the table in the recyclebin
 */
void
polar_flashback_drop(PlannedStmt *pstmt,
					 const char *queryString,
					 ProcessUtilityContext context,
					 ParamListInfo params,
					 QueryEnvironment *queryEnv,
					 DestReceiver *dest,
					 char *completionTag)
{
	Relation    rel;
	Oid         out_NspOid;
	RangeVar    *arr_r[20];
	Oid         arr_reloid_p[20];
	Oid         arr_reloid[20];
	char        *trashcan_nspname;
	char        *schema_name;
	int         i;
	DropStmt    *stmt;

	trashcan_nspname = RECYCLEBINNAME;
	stmt = (DropStmt *) pstmt->utilityStmt;

	if (stmt->removeType == OBJECT_TABLE && stmt->behavior == DROP_RESTRICT)
	{
		if (stmt->purge_drop)
			polar_purge_table(stmt, trashcan_nspname);
		else
		{
			int number_object_p = 0;
			int number_object = 0;
			bool isInvalid = false;

			polar_find_partitionAndInvalidOid(stmt, &number_object_p, &number_object,
											  &isInvalid, arr_r, arr_reloid_p, arr_reloid);

			if (isInvalid)
			{
				standard_ProcessUtility(pstmt, queryString,
										context, params, queryEnv,
										dest, completionTag);
			}
			else
			{
				if (number_object > 0)
					polar_report_dependency(arr_reloid, number_object);

				if (number_object_p > 0)
					polar_drop_table(arr_reloid_p, 0, number_object_p);

				for (i = 0; i < number_object; i++)
				{
					rel = RelationIdGetRelation(arr_reloid[i]);

					if (rel)
					{
						out_NspOid = RelationGetNamespace(rel);
						schema_name = get_namespace_name(out_NspOid);
						RelationClose(rel);

						/*two case: 1.temp table ;2.drop table in recyclebin */
						if (!isAnyTempNamespace(out_NspOid) && strcmp(schema_name, trashcan_nspname) != 0)
						{
							polar_drop_to_recyclebin(pstmt, queryString,
													 context, params, queryEnv,
													 dest, completionTag,
													 stmt, arr_r,
													 schema_name, out_NspOid, i);
						}
						else
						{
							polar_deal_tempAndRecyclebin_table(pstmt, queryString, context,
															   params, queryEnv,
															   dest, completionTag,
															   stmt, arr_r,
															   arr_reloid, out_NspOid, i);
						}
					}
				}
			}
		}
	}
	else
		standard_ProcessUtility(pstmt, queryString,
								context, params, queryEnv,
								dest, completionTag);
}

/*
 *POLAR: recovery table, recovery the table from the recyclebin
 */
void
polar_flashback_recover_table(PlannedStmt *pstmt,
					  const char *queryString,
					  ProcessUtilityContext context,
					  ParamListInfo params,
					  QueryEnvironment *queryEnv,
					  DestReceiver *dest,
					  char *completionTag)
{
	char        *table_name = NULL;
	char        *new_table_name;
	char        *schema_name;
	RangeVar    *r;
	Oid         nspOid;
	RangeVar    *currentrv;
	char        *trashcan_nspname;

	AlterObjectSchemaStmt   *newstmt ;
	AlterObjectSchemaStmt   *stmt ;

	trashcan_nspname = RECYCLEBINNAME;
	newstmt = makeNode(AlterObjectSchemaStmt);
	stmt = (AlterObjectSchemaStmt *) pstmt->utilityStmt;

	newstmt->objectType = OBJECT_TABLE;
	newstmt->missing_ok = stmt->missing_ok;

	if (!stmt->relation->schemaname)
	{
		/*Get the current schema name*/
		currentrv = makeRangeVar(NULL, NULL, -1);
		nspOid = RangeVarGetAndCheckCreationNamespace(currentrv, NoLock, NULL);
		schema_name = get_namespace_name(nspOid);
	}
	else
	{
		if (strcmp(stmt->relation->schemaname, trashcan_nspname) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("can not flashback table in recyclebin")));
		else
			schema_name = stmt->relation->schemaname;
	}

	table_name = polar_get_table_according_time(schema_name, stmt->relation->relname, true);

	if (!table_name)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" do not exist in recyclebin ", stmt->relation->relname)));

	if (stmt->newtablename)
		new_table_name = stmt->newtablename;
	else
		new_table_name = stmt->relation->relname;

	stmt->relation->relname = table_name;
	stmt->relation->schemaname = trashcan_nspname;

	polar_rename_table(stmt->relation, new_table_name);

	r = makeRangeVar(stmt->relation->schemaname, new_table_name, -1);
	newstmt->relation = r;
	pstmt->utilityStmt = (Node *) newstmt;

	standard_ProcessUtility(pstmt, queryString,
							context, params, queryEnv,
							dest, completionTag);
	pfree(newstmt);
}

/*
 *POLAR: when polar_enable_flashback_drop = off,report error
 */
static bool
polar_check_stmt_flashback_opt(PlannedStmt *pstmt)
{
	if (nodeTag(pstmt->utilityStmt) == T_DropStmt && ((DropStmt *)pstmt->utilityStmt)->removeType == OBJECT_TABLE && ((DropStmt *)pstmt->utilityStmt)->opt_flashback)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("flashback_table function is not turned on")));
		return true;
	}
	else if (nodeTag(pstmt->utilityStmt) == T_AlterObjectSchemaStmt && ((AlterObjectSchemaStmt *)pstmt->utilityStmt)->is_flashback)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("flashback_table function is not turned on")));
		return true;
	}
	else
		return false;
}

/*
 *POLAR: when polar_enable_flashback_drop = on,execute
 */
bool
polar_flashback_drop_process_utility(PlannedStmt *pstmt,
										const char *queryString,
										ProcessUtilityContext context,
										ParamListInfo params,
										QueryEnvironment *queryEnv,
										DestReceiver *dest,
										char *completionTag,
										bool ishook)
{
	if (polar_enable_flashback_drop)
	{
		Node *node;
		node = pstmt->utilityStmt;

		if (nodeTag(node) == T_DropStmt && !((DropStmt *) node)->ispurge && !((DropStmt *) node)->clean_up)
		{
			if(ishook)
				ereport(WARNING,
				       (errmsg("flashback_table function is turned on, drop will follow flashback_drop logic")));
					   
			polar_flashback_drop(pstmt, queryString,
								 context, params, queryEnv,
								 dest, completionTag);
			return true;
		}
		else if (nodeTag(node) == T_AlterObjectSchemaStmt && ((AlterObjectSchemaStmt *)node)->is_flashback)
		{
			polar_flashback_recover_table(pstmt, queryString,
								  context, params, queryEnv,
								  dest, completionTag);
			return true;
		}
		else if (nodeTag(node) == T_DropStmt && ((DropStmt *) node)->clean_up)
		{
			if (superuser() || polar_superuser())
			{
				standard_ProcessUtility(pstmt, queryString,
										context, params, queryEnv,
										dest, completionTag);

				polar_create_grant_recyclebin(RECYCLEBINNAME);

				return true;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 (errmsg("only superuser can purge recyclebin"))));
		}
		else
			return false;
	}
	else
		return polar_check_stmt_flashback_opt(pstmt);
}