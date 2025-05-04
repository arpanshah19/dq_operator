import os
import json
from airflow.models.baseoperator import BaseOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.context import Context
from typing import Callable, Optional

class DQQueryBuilder:
    DQ_TABLE = "DQ_SUMMARY"

    @staticmethod
    def validate(check_type,
        main_table,
        column_name,
        compare_table,
        source_query,
        target_query,
        date_column):

        if check_type == "EXCEPT":
            if not ((main_table and compare_table) or (source_query and target_query)):
                raise ValueError("EXCEPT check requires either main_table and compare_table OR source_query and target_query.")
        
        elif check_type == "RECON":
            if not ((main_table and compare_table) or (source_query and target_query)):
                raise ValueError("RECON check requires either main_table and compare_table OR source_query and target_query.")

        elif check_type == "TREND":
            if not column_name:
                raise ValueError("TREND check requires column_name.")
            if not ((main_table and compare_table and date_column) or (source_query and target_query)):
                raise ValueError("TREND check requires either main_table, compare_table, and date_column OR source_query and target_query.")

        elif check_type == "NULL":
            if not (main_table and column_name):
                raise ValueError("NULL check requires main_table and column_name.")

        elif check_type == "CUSTOM":
            if not source_query:
                raise ValueError("CUSTOM check requires source_query.")

        else:
            raise ValueError(f"Unsupported check_type: {check_type}")

    @staticmethod
    def check(
        check_type,
        check_name,
        main_table=None,
        column_name=None,
        agg_function='COUNT',
        compare_table=None,
        source_query=None,
        target_query=None,
        threshhold=0,
        business_date='current_date',
        date_column=None
    ):
        check_type = check_type.upper()
        
        DQQueryBuilder.validate(check_type, main_table, column_name, compare_table, source_query, target_query, date_column)

        query_generator = {
            'EXCEPT': DQQueryBuilder._except_query,
            'RECON': DQQueryBuilder._recon_query,
            'TREND': DQQueryBuilder._trend_query,
            'NULL': DQQueryBuilder._null_query,
            'CUSTOM': DQQueryBuilder._custom_query
        }.get(check_type)

        if not query_generator:
            raise ValueError(f"Unsupported check_type: {check_type}")

        return query_generator(
            check_name=check_name,
            main_table=main_table,
            column_name=column_name,
            agg_function=agg_function,
            compare_table=compare_table,
            source_query=source_query,
            target_query=target_query,
            threshhold=threshhold,
            business_date=business_date,
            date_column=date_column
        )

    @staticmethod
    def _base_select_block(check_type, check_name, main_table, compare_table, column_name, business_date):
        return f"""
            SELECT
                '{check_type}' AS check_type,
                '{check_name}' AS check_name,
                '{main_table}' AS main_table,
                {f"'{compare_table}'" if compare_table else "NULL"} AS compare_table,
                {f"'{column_name}'" if column_name else "NULL"} AS column_name,
                status_check.status,
                status_check.analysis,
                {business_date if business_date == 'current_date' else f"'{business_date}'"} AS business_date,
                CURRENT_TIMESTAMP AS row_created_ts,
                CURRENT_DATE AS row_created_dt
            FROM status_check
        """

    @staticmethod
    def _except_query(**kwargs):
        col = kwargs['column_name'] or '*'
        src = kwargs['source_query'] or f"SELECT {col} FROM {kwargs['main_table']}"
        tgt = kwargs['target_query'] or f"SELECT {col} FROM {kwargs['compare_table']}"
        return f"""
            INSERT INTO {DQQueryBuilder.DQ_TABLE}
            WITH diff AS (
                ({src} EXCEPT {tgt})
                UNION
                ({tgt} EXCEPT {src})
            ),
            status_check AS (
                SELECT CASE WHEN COUNT(*) > 0 THEN 'Failed' ELSE 'Passed' END AS status,
                    'EXCEPT check: ' || COUNT(*) || ' differing rows found.' AS analysis
                FROM diff
            )
            {DQQueryBuilder._base_select_block("EXCEPT", kwargs['check_name'], kwargs['main_table'], kwargs['compare_table'], kwargs['column_name'], kwargs['business_date'])}
        """

    @staticmethod
    def _recon_query(**kwargs):
        where = f" WHERE {kwargs['date_column']} = {kwargs['business_date']}" if kwargs['date_column'] else ""
        column_expr = kwargs.get('column_name', '*')
        if column_expr == '*':
            agg_expr = f"{kwargs['agg_function']}(*)"
        else:
            agg_expr = f"{kwargs['agg_function']}({column_expr})"

        src_query = kwargs.get('source_query') or f"SELECT {agg_expr} AS value FROM {kwargs['main_table']}{where}"
        tgt_query = kwargs.get('target_query') or f"SELECT {agg_expr} AS value FROM {kwargs['compare_table']}{where}"
        return f"""
            INSERT INTO {DQQueryBuilder.DQ_TABLE}
            WITH main_agg AS ({src_query}),
                compare_agg AS ({tgt_query}),
            status_check AS (
                SELECT CASE WHEN m.value = c.value THEN 'Passed' ELSE 'Failed' END AS status,
                    'RECON check: Main = ' || m.value || ', Compare = ' || c.value AS analysis
                FROM main_agg m, compare_agg c
            )
            {DQQueryBuilder._base_select_block("RECON", kwargs['check_name'], kwargs['main_table'], kwargs['compare_table'], None, kwargs['business_date'])}
        """

    @staticmethod
    def _trend_query(**kwargs):
        business_date = kwargs['business_date']
        date_column = kwargs['date_column']
        column_name = kwargs['column_name']
        agg_function = kwargs['agg_function']
        threshold = kwargs['threshhold']

        today_query = kwargs['source_query'] or f"""
            SELECT {agg_function}({column_name}) AS value FROM {kwargs['main_table']} 
            WHERE {date_column} = {business_date}
            """
        prev_query = kwargs['target_query'] or f"""
            SELECT AVG(trend) AS value FROM (
                SELECT {agg_function}({column_name}) AS trend
                FROM {kwargs['compare_table']}
                WHERE {date_column} <> {business_date} AND {date_column} > {business_date} - 7
            )
            """

        return f"""
            INSERT INTO {DQQueryBuilder.DQ_TABLE}
            WITH today_val AS ({today_query}),
                prev_val AS ({prev_query}),
            status_check AS (
                SELECT CASE
                    WHEN p.value = 0 THEN 'Passed'
                    WHEN ABS(t.value - p.value)*100.0 / p.value > {threshold} THEN 'Failed'
                    ELSE 'Passed'
                END AS status,
                'TREND check: Current = ' || t.value || ', Previous = ' || p.value || ', Threshold = {threshold}%' AS analysis
                FROM today_val t, prev_val p
            )
            {DQQueryBuilder._base_select_block("TREND", kwargs['check_name'], kwargs['main_table'], kwargs['compare_table'], column_name, business_date)}
        """

    @staticmethod
    def _null_query(**kwargs):
        where = f"WHERE {kwargs['column_name']} IS NULL"
        if kwargs['date_column']:
            where += f" AND {kwargs['date_column']} = {kwargs['business_date']}"
        return f"""
            INSERT INTO {DQQueryBuilder.DQ_TABLE}
            WITH null_check AS (
                SELECT COUNT(*) AS null_count FROM {kwargs['main_table']} {where}
            ),
            status_check AS (
                SELECT CASE WHEN null_count > 0 THEN 'Failed' ELSE 'Passed' END AS status,
                    'NULL check: ' || null_count || ' nulls found in column {kwargs['column_name']}' AS analysis
                FROM null_check
            )
            {DQQueryBuilder._base_select_block("NULL", kwargs['check_name'], kwargs['main_table'], None, kwargs['column_name'], kwargs['business_date'])}
        """

    @staticmethod
    def _custom_query(**kwargs):
        return f"""
            INSERT INTO {DQQueryBuilder.DQ_TABLE}
            WITH custom_check AS (
                {kwargs['main_table']}
            ),
            status_check AS (
                SELECT CASE WHEN COUNT(*) > 0 THEN 'Failed' ELSE 'Passed' END AS status,
                    'CUSTOM check: ' || COUNT(*) || ' issues found' AS analysis
                FROM custom_check
            )
            {DQQueryBuilder._base_select_block("CUSTOM", kwargs['check_name'], kwargs['main_table'], None, None, kwargs['business_date'])}
        """


class DataQualityOperator(BaseOperator):
    """
    Operator to generate and execute a single Data Quality check.
    """

    def __init__(
        self,
        dq_config: dict,
        sql_executor: Callable[[str], None],
        dq_table: str,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.dq_config = dq_config
        self.sql_executor = sql_executor

    def execute(self, context: Context):
        DQQueryBuilder.DQ_TABLE = dq_table
        check_name = self.dq_config.get("check_name", "Unnamed DQ Check")
        self.log.info(f"Running DQ Check: {check_name}")
        sql = DQQueryBuilder.check(**self.dq_config)
        self.log.debug(f"Generated SQL:\n{sql}")
        self.sql_executor(sql)
        self.log.info(f"Check '{check_name}' completed.")


def create_dq_taskgroup(
    group_id: str,
    sql_executor: Callable[[str], None],
    config_path: Optional[str] = None,
    dag_dir: Optional[str] = None,
    dq_table: str= "dq_summary"
) -> TaskGroup:
    """
    Create a TaskGroup for DQ checks defined in a dq_config.json file.

    Parameters:
        - group_id: TaskGroup ID
        - sql_executor: A callable to execute SQL (e.g., wrapper around SqlHook)
        - config_path: Optional full path to dq_config.json
        - dag_dir: If config_path is not passed, assume dq_config.json is in the same folder as the DAG
    """

    if config_path is None:
        dag_dir = dag_dir or os.path.dirname(__file__)
        config_path = os.path.join(dag_dir, "dq_config.json")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Missing dq_config.json at: {config_path}")

    with open(config_path) as f:
        dq_data = json.load(f)

    checks = dq_data.get("checks", [])
    if not checks:
        raise ValueError("No DQ checks found in dq_config.json.")

    with TaskGroup(group_id=group_id) as dq_group:
        for idx, dq_conf in enumerate(checks):
            task_id = f"{dq_conf.get('check_type', 'unknown').upper()}-{dq_conf.get('check_name', 'unknown').upper().replace(' ', '_')}"
            DataQualityOperator(
                task_id=task_id,
                dq_config=dq_conf,
                sql_executor=sql_executor,
                dq_table= dq_table
            )

    return dq_group
