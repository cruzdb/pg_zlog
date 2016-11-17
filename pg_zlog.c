#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_init(void)
{
	ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		errmsg("hello"), errhint("world")));
}
