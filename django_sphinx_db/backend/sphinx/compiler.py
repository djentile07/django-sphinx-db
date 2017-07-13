from django.db.models.sql import compiler
from django.db.models.sql.where import WhereNode
from django.db.models.expressions import Col
EmptyShortCircuit = Exception
EmptyResultSet = Exception
#from django.db.models.sql.expressions import SQLEvaluator


class SphinxWhereNode(WhereNode):
    def sql_for_columns(self, data, qn, connection):
        table_alias, name, db_type = data
        logging.error("t:{} n:{} d:".format(table_alias, name, db_type))
        return connection.ops.field_cast_sql(db_type) % name

    def as_sql(self, qn, connection):
        # TODO: remove this when no longer needed.
        # This is to remove the parenthesis from where clauses.
        # http://sphinxsearch.com/bugs/view.php?id=1150
        
        sql, params = super(SphinxWhereNode, self).as_sql(qn, connection)
        if sql and sql[0] == '(' and sql[-1] == ')':
            # Trim leading and trailing parenthesis:
            sql = sql[1:]
            sql = sql[:-1]
        logging.error("SQL:{} {}".format(sql, params))
        return sql, params

    def make_atom(self, child, qn, connection):
        """
        Transform search, the keyword should not be quoted.
        """
        lvalue, lookup_type, value_annot, params_or_value = child
        sql, params = super(SphinxWhereNode, self).make_atom(child, qn, connection)
        if lookup_type == 'search':
            if hasattr(lvalue, 'process'):
                try:
                    lvalue, params = lvalue.process(lookup_type, params_or_value, connection)
                except EmptyShortCircuit:
                    raise EmptyResultSet
            if isinstance(lvalue, tuple):
                # A direct database column lookup.
                field_sql = self.sql_for_columns(lvalue, qn, connection)
            else:
                # A smart object with an as_sql() method.
                field_sql = lvalue.as_sql(qn, connection)
            # TODO: There are a couple problems here.
            # 1. The user _might_ want to search only a specific field.
            # 2. However, since Django requires a field name to use the __search operator
            #    There is no way to do a search in _all_ fields.
            # 3. Because, using multiple __search operators is not supported.
            # So, we need to merge multiped __search operators into a single MATCH(), we
            # can't do that here, we have to do that one level up...
            # Ignore the field name, search all fields:
            params = ('@* %s' % params[0], )
            # _OR_ respect the field name, and search on it:
            #params = ('@%s %s' % (field_sql, params[0]), )
        return sql, params


class SphinxQLCompiler(compiler.SQLCompiler):
    def get_columns(self, *args, **kwargs):
        columns = super(SphinxQLCompiler, self).get_columns(*args, **kwargs)
        for i, column in enumerate(columns):
            if '.' in column:
                columns[i] = column.partition('.')[2]
        return columns

    def quote_name_unless_alias(self, name):
        # TODO: remove this when no longer needed.
        # This is to remove the `` backticks from identifiers.
        # http://sphinxsearch.com/bugs/view.php?id=1150
        return name


    def compile(self, node, select_format=False):
        retval = super(SphinxQLCompiler, self).compile(node, select_format=select_format)
        if isinstance(node, Col):
            sql, params = retval
            sql = sql.split(".")[-1] 
            retval = (sql, params)
        return retval

    def as_sql(self, with_limits=True, with_col_aliases=False, subquery=False):
        """
        Creates the SQL for this query. Returns the SQL string and list of
        parameters.

        If 'with_limits' is False, any limit/offset information is not included
        in the query.
        """
        self.subquery = subquery
        refcounts_before = self.query.alias_refcount.copy()
        try:
            extra_select, order_by, group_by = self.pre_sql_setup()
            distinct_fields = self.get_distinct()

            # This must come after 'select', 'ordering', and 'distinct' -- see
            # docstring of get_from_clause() for details.
            from_, f_params = self.get_from_clause()

            where, w_params = self.compile(self.where) if self.where is not None else ("", [])
            having, h_params = self.compile(self.having) if self.having is not None else ("", [])
            params = []
            result = ['SELECT']

            if self.query.distinct:
                result.append(self.connection.ops.distinct_sql(distinct_fields))

            out_cols = []
            col_idx = 1
            for _, (s_sql, s_params), alias in self.select + extra_select:
                if alias:
                    s_sql = '%s AS %s' % (s_sql, self.connection.ops.quote_name(alias))
                elif with_col_aliases:
                    s_sql = '%s AS %s' % (s_sql, 'Col%d' % col_idx)
                    col_idx += 1
                params.extend(s_params)
                out_cols.append(s_sql)

            result.append(', '.join(out_cols))

            result.append('FROM')
            result.extend(from_)
            params.extend(f_params)

            if where:
                result.append('WHERE %s' % where)
                params.extend(w_params)

            grouping = []
            for g_sql, g_params in group_by:
                grouping.append(g_sql)
                params.extend(g_params)
            if grouping:
                if distinct_fields:
                    raise NotImplementedError(
                        "annotate() + distinct(fields) is not implemented.")
                if not order_by:
                    order_by = self.connection.ops.force_no_ordering()
                result.append('GROUP BY %s' % ', '.join(grouping))

            if having:
                result.append('HAVING %s' % having)
                params.extend(h_params)

            if order_by:
                ordering = []
                for _, (o_sql, o_params, _) in order_by:
                    ordering.append(o_sql)
                    params.extend(o_params)
                result.append('ORDER BY %s' % ', '.join(ordering))

            if with_limits:
                result.append("LIMIT")
                if self.query.low_mark:
                    result.append("%d," % self.query.low_mark)
                limit = self.query.high_mark
                if not limit:
                    limit = 1000
                             
                result.append('%d' % (limit))

            if self.query.select_for_update and self.connection.features.has_select_for_update:
                if self.connection.get_autocommit():
                    raise TransactionManagementError(
                        "select_for_update cannot be used outside of a transaction."
                    )

                # If we've been asked for a NOWAIT query but the backend does
                # not support it, raise a DatabaseError otherwise we could get
                # an unexpected deadlock.
                nowait = self.query.select_for_update_nowait
                if nowait and not self.connection.features.has_select_for_update_nowait:
                    raise DatabaseError('NOWAIT is not supported on this database backend.')
                result.append(self.connection.ops.for_update_sql(nowait=nowait))

            return ' '.join(result), tuple(params)
        finally:
            # Finally do cleanup - get rid of the joins we created above.
            self.query.reset_refcounts(refcounts_before)

# Set SQLCompiler appropriately, so queries will use the correct compiler.
SQLCompiler = SphinxQLCompiler


class SQLInsertCompiler(compiler.SQLInsertCompiler, SphinxQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SphinxQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SphinxQLCompiler):
    def as_sql(self):
        qn = self.connection.ops.quote_name
        opts = self.query.model._meta
        result = ['REPLACE INTO %s' % qn(opts.db_table)]
        # This is a bit ugly, we have to scrape information from the where clause
        # and put it into the field/values list. Sphinx will not accept an UPDATE
        # statement that includes full text data, only INSERT/REPLACE INTO.
        lvalue, lookup_type, value_annot, params_or_value = self.query.where.children[0].children[0]
        (table_name, column_name, column_type), val = lvalue.process(lookup_type, params_or_value, self.connection)
        fields, values, params = [column_name], ['%s'], [val[0]]
        # Now build the rest of the fields into our query.
        for field, model, val in self.query.values:
            if hasattr(val, 'prepare_database_save'):
                val = val.prepare_database_save(field)
            else:
                val = field.get_db_prep_save(val, connection=self.connection)

            # Getting the placeholder for the field.
            if hasattr(field, 'get_placeholder'):
                placeholder = field.get_placeholder(val, self.connection)
            else:
                placeholder = '%s'

            if hasattr(val, 'evaluate'):
                logging.error("SQL Evaluator was deprecated!!!")
            #    val = SQLEvaluator(val, self.query, allow_joins=False)
            name = field.column
            if hasattr(val, 'as_sql'):
                sql, params = val.as_sql(qn, self.connection)
                values.append(sql)
                params.extend(params)
            elif val is not None:
                values.append(placeholder)
                params.append(val)
            else:
                values.append('NULL')
            fields.append(name)
        result.append('(%s)' % ', '.join(fields))
        result.append('VALUES (%s)' % ', '.join(values))
        return ' '.join(result), params


class SQLAggregateCompiler(SphinxQLCompiler):
    pass

class SQLDateCompiler(SphinxQLCompiler):
    pass
