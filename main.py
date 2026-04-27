import clickhouse_connect.driver.client
import pandas as pd
import time
import traceback
from datetime import datetime, timedelta
import time_calc
import warnings
import clickhouse_connect
from clickhouse_connect.driver import httputil

warnings.simplefilter("ignore", FutureWarning)

_client = None

def get_clickhouse_type(dtype) -> str:  # type: ignore
    """根据 pandas dtype 推断 ClickHouse 类型"""
    if pd.api.types.is_integer_dtype(dtype):
        return 'Int64'
    if pd.api.types.is_float_dtype(dtype):
        return 'Float64'
    if pd.api.types.is_bool_dtype(dtype):
        return 'UInt8'  # ClickHouse 中常用 UInt8 表示布尔
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DateTime64(6)'
    # 默认字符串，能兼容中文、混合类型等
    return 'String'


def create_table_from_df(
    client,
    df: pd.DataFrame,
    table_name: str,
    database: str = "default"
):
    """根据 DataFrame 自动生成并执行 CREATE TABLE"""
    columns_def = []
    for col_name, dtype in df.dtypes.items():
        ch_type = get_clickhouse_type(dtype)
        # 中文/特殊列名用双引号包裹，避免 ClickHouse 解析错误
        safe_col = f'"{col_name}"'
        columns_def.append(f"    {safe_col} {ch_type}")

    # 如果列很多，用 tuple() 作为 ORDER BY（无排序键）
    # 如果有合适的列（如 ID、时间），可以改成 ORDER BY ("xxx")
    sql = f"""CREATE TABLE IF NOT EXISTS {database}.{table_name} (
{',\n'.join(columns_def)}
) ENGINE = MergeTree()
ORDER BY tuple()
"""
    client.command(sql)
    print(f"已确保表存在: {database}.{table_name} ({len(df.columns)} 列)")
_client = None


def get_client() -> clickhouse_connect.driver.client.Client:
    global _client
    if _client is None:
        pool_mgr = httputil.get_pool_manager(maxsize=32, num_pools=2, block=True, timeout=300)
        _client = clickhouse_connect.get_client(
            host="10.24.5.59", port=8123, username="cheakf", password="Swq8855830.",
            database="default", pool_mgr=pool_mgr
        )
    return _client


def main():
    local = get_client()

    filter_time = pd.Timestamp("2025-1-6 00:00:00")
    response_limit = timedelta(hours=2)  # 响应超时时间
    handle_limit = timedelta(hours=8)  # 处理超时时间

    def _is_workday(input_time: datetime):
        dd = local.query_df(f"""
                SELECT 
                    bill.是否休息
                FROM 
                    (        
                        SELECT
                            *
                        FROM
                            ods.attendance_kq_scheduling_holiday
                        WHERE
                            Deleted = 0 QUALIFY row_number() OVER (
                                PARTITION BY
                                    zid
                                ORDER BY
                                    Version DESC
                            ) = 1
                    ) AS bill
                WHERE toDate(bill."节假日日期") = toDate('{input_time.strftime(r"%Y-%m-%d %H:%M:%S.%f")}')
            """)
        # 如果有额外的设定，就按照要求返回结果
        if len(dd) > 0:
            return not bool(dd[0])
        # 如果没有额外的节假日，就按照周一到周五上班返回
        return input_time.weekday() < 5
    time_calc.is_workday = _is_workday

    local.command("""
        CREATE TABLE IF NOT EXISTS dwd.cg_mes_usm_exception_cache (
            fzid VARCHAR PRIMARY KEY
        )""")
    cache_columns = ["fzid", "计算完成", "应响应时间", "响应是否超时", "应处理时间", "处理是否超时"]
    exc_bill = local.query_df(f"""
            SELECT 
                bill.*, 
                cache.*
            FROM                     
            (        
                SELECT
                    *
                FROM
                    ods.cg_mes_usm_exception_bill
                WHERE
                    Deleted = 0 QUALIFY row_number() OVER (
                        PARTITION BY
                            zid
                        ORDER BY
                            Version DESC
                    ) = 1
            ) AS bill
            LEFT JOIN dwd.cg_mes_usm_exception_cache AS cache ON bill."zid" = cache."fzid"
            WHERE bill."创建日期" > '{filter_time.strftime(r"%Y-%m-%d %H:%M:%S.%f")}'
        """)
    cols: list[list] = []
    for _, row in exc_bill.iterrows():
        if (not pd.isnull(row.get("计算完成"))) and bool(row.get("计算完成")) == True:
            cols.append([
                row[k] for k in cache_columns
            ])
            continue
        request = pd.Timestamp(row["发起日期"]).to_pydatetime()
        response_deadline = time_calc.worktime_add(request, response_limit)  # 响应最后期限
        handle_deadline = time_calc.worktime_add(response_deadline, handle_limit)  # 处理最后期限
        response_overtime = False  # 响应是否超时
        if pd.isnull(row["响应日期"]):
            response_overtime = datetime.now() > response_deadline
        else:
            response = pd.Timestamp(row["响应日期"]).to_pydatetime()
            response_overtime = response > response_deadline
            handle_deadline = time_calc.worktime_add(response, handle_limit)  # 响应后指定时间内处理
        handle_overtime = False  # 处理是否超时
        if pd.isnull(row["处理日期"]):
            handle_overtime = datetime.now() > handle_deadline
        else:
            handle = pd.Timestamp(row["处理日期"]).to_pydatetime()
            handle_overtime = handle > handle_deadline
        finished = False
        if row["异常状态分类"] == "待关闭" or row["异常状态分类"] == "已关闭":
            finished = True
        cols.append([str(row["zid"]),   finished,
                     response_deadline, '否' if not response_overtime else '是',
                     handle_deadline,   '否'if not handle_overtime else '是'
                     ])

    time_judge = pd.DataFrame(cols, columns=cache_columns)
    create_table_from_df(local, time_judge, "cg_mes_usm_exception_cache", "dwd")
    # local.command("""
    #     CREATE OR REPLACE TABLE dwd.cg_mes_usm_exception_cache 
    #     AS SELECT * FROM time_judge""")
    local.command("""
        CREATE OR REPLACE TABLE dwd.cg_mes_usm_exception_processed AS
        WITH emp_org AS ( 
            SELECT
                emp."工号",
                emp."姓名", 
                org."组织名称" AS "组室",
                org_p."组织名称" AS "部门"
            FROM "ods"."person_employee" AS emp 
            LEFT JOIN "ods"."person_organization" AS org ON emp."所属组织id" = org.zid
            LEFT JOIN "ods"."person_organization" AS org_p ON org."上级组织id" = org_p.zid
            WHERE
                emp."任职状态" != '离职'
                AND emp."任职状态" != '退休'
                AND org."组织状态" = '启用'
        ) SELECT
            "bill"."zid",
            "bill"."异常类型",
            "bill"."异常类型编码",
            "bill"."异常内容名称",
            "bill"."异常描述",
            "bill"."异常状态",
            "bill"."项目",
            "bill"."节车号",
            "bill"."车号",
            "bill"."工作中心",
            "bill"."工位",
            "bill"."工序名称",
            "bill"."工序编码",
            "bill"."异常内容描述",
            "bill"."工位主键",
            "bill"."发起人",
            "bill"."发起日期",
            "bill"."创建日期",
            "bill"."修改日期",
            "bill"."指定响应人",
            "bill"."响应人",
            "bill"."响应日期",
            "bill"."处理人",
            "bill"."处理日期",
            "bill"."关闭人",
            "bill"."关闭日期",
            "bill"."异常状态分类",
            "t"."应响应时间",
            "t"."响应是否超时",
            "t"."应处理时间",
            "t"."处理是否超时",
            "reporter"."姓名" AS "发起人姓名",
            "reporter"."组室" AS "发起人组室",
            "reporter"."部门" AS "发起人部门",
            "appoint"."姓名" AS "指定响应人姓名",
            "appoint"."组室" AS "指定响应人组室",
            "appoint"."部门" AS "指定响应人部门",
            "response"."姓名" AS "响应人姓名",
            "response"."组室" AS "响应人组室",
            "response"."部门" AS "响应人部门",
            "handle"."姓名" AS "处理人姓名",
            "handle"."组室" AS "处理人组室",
            "handle"."部门" AS "处理人部门",
            "closer"."姓名" AS "关闭人姓名",
            "closer"."组室" AS "关闭人组室",
            "closer"."部门" AS "关闭人部门",
            "pro"."项目名称" AS "项目名称"
        FROM "exc_bill" AS "bill"
        LEFT JOIN dwd.cg_mes_usm_exception_cache AS "t" ON "bill"."zid" = "t"."fzid"
        LEFT JOIN "emp_org" AS "reporter" ON "bill"."发起人" = "reporter"."工号"
        LEFT JOIN "emp_org" AS "appoint" ON "bill"."指定响应人" = "appoint"."工号"
        LEFT JOIN "emp_org" AS "response" ON "bill"."响应人"="response"."工号" 
        LEFT JOIN "emp_org" AS "handle" ON "bill"."处理人"= "handle"."工号"
        LEFT JOIN "emp_org" AS "closer" ON "bill"."关闭人"= "closer"."工号"
        LEFT JOIN (
            SELECT
                *
            FROM
                ods.crrc_project1
            WHERE
                Deleted = 0 QUALIFY row_number() OVER (
                    PARTITION BY
                        zid
                    ORDER BY
                        Version DESC
                ) = 1
        ) AS "pro" ON "bill"."项目" = "pro"."项目号"
    """)
    
def run():
    while True:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 生产异常开始计算")
        try:
            main()
        except Exception:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] error:{traceback.format_exc()}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 生产异常计算完成，等待1分钟......")
        time.sleep(60)

if __name__ == "__main__":
    main()
