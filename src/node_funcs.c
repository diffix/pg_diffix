#include "postgres.h"

#include "pg_diffix/node_funcs.h"
#include "pg_diffix/utils.h"

static ParamExternData *get_param_data(ParamListInfo bound_params, int one_based_paramid)
{
  if (bound_params->paramFetch != NULL)
    return bound_params->paramFetch(bound_params, one_based_paramid - 1, true, NULL);
  else
    return &bound_params->params[one_based_paramid - 1];
}

bool is_simple_constant(Node *node)
{
  return IsA(node, Const) || (IsA(node, Param) && ((Param *)node)->paramkind == PARAM_EXTERN);
}

void get_simple_constant_typed_value(Node *node, ParamListInfo bound_params, Oid *type, Datum *value, bool *isnull)
{
  if (IsA(node, Const))
  {
    Const *const_expr = (Const *)node;
    *type = const_expr->consttype;
    *value = const_expr->constvalue;
    *isnull = const_expr->constisnull;
  }
  else if (IsA(node, Param) && ((Param *)node)->paramkind == PARAM_EXTERN)
  {
    Param *param_expr = (Param *)node;
    ParamExternData *param_data = get_param_data(bound_params, param_expr->paramid);
    *type = param_data->ptype;
    *value = param_data->value;
    *isnull = param_data->isnull;
  }
  else
  {
    FAILWITH("Attempted to get simple constant value of non-Const, non-PARAM_EXTERN node");
  }
}
