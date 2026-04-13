from sys import argv
import os
import time
import cx_Oracle
from sqlalchemy import types, create_engine
import numpy as np
import pandas as pd
import networkx as nx
import multiprocessing as multi

df_srv = pd.DataFrame()
df_ir = pd.DataFrame()
df_as = pd.DataFrame()
df_links = pd.DataFrame()
df_srv_ke = pd.DataFrame()
df_ir_ke = pd.DataFrame()
df_as_ke = pd.DataFrame()

p_to_c = nx.DiGraph()
c_to_p = nx.DiGraph()

# query to retrieve object relationships
query_links = """
    select r.LOGICAL_NAME as CI_PARENT, r.TPS_RELATED_CIS as CI_CHILD
    from CIRELATIONSM1 r
    where exists (
            select LOGICAL_NAME
            from DEVICE2M1 d
            where  d.HPC_STATUS != 'Out of Service'
                and (d.TYPE in ('server', 'infresource', 'bizservice', 'dbmsinstance', 'collection', 'environmenttype', 'cluster', 'sbvirtcluster')
                    or d.HPC_DEVICE_TYPE_RUS = 'AS')
                and (d.LOGICAL_NAME = r.LOGICAL_NAME or d.LOGICAL_NAME = r.TPS_RELATED_CIS)
            ) 
"""

# query to retrieve server objects
query_srv = """
    select srvr.LOGICAL_NAME as LOGICAL_NAME,
        srvr.hpc_device_type_rus as KAT_SERVER,
        srvr.subtype as SUBTYPE,
        srvr.operated_in_di as ISSUED_FROM_DI,
        TRIM(BOTH chr(13) from TRIM(BOTH chr(10) from  TRIM(BOTH chr(22) from srvr.tps_name))) as HOST, 
        (select listagg(servip.ip_addresses,';') within group (order by servip.ip_addresses)
            FROM DEVICE2A2 servip
            WHERE servip.LOGICAL_NAME = srvr.LOGICAL_NAME 
            fetch first 10 rows only)||';' as IP,
        replace(
            replace(
            replace(
            replace(
                (TRIM(';' from (select listagg(dns.dns_alias,', ') within group (order by dns.dns_alias)
                                FROM DEVICE2A4 dns
                                WHERE dns.LOGICAL_NAME = srvr.logical_name
                                fetch first 50 rows only
                                )
                ||chr(9)))
            ,CHR(9),'')
            ,chr(10),'')
            ,chr(13),'')
            ,chr(32),'') as DNS_ALIAS,
        '' as DNS_ALIAS_D,
        srvr.operating_system as OS,
        srvr.environment as ENVIRONMENT,
        srvr.tps_dns_name as DOMAIN,
        (select servCPU.J_CPU_COUNT
            FROM  sbjserverm1 servCPU
            WHERE servCPU.LOGICAL_NAME = srvr.LOGICAL_NAME) as CPU_COUNT,
        (select servRAM.J_RAM
            FROM sbjserverm1 servRAM
            WHERE servRAM.LOGICAL_NAME = srvr.LOGICAL_NAME) as RAM,
        (select servHDD.J_HDD
            FROM sbjserverm1 servHDD
            WHERE servHDD.LOGICAL_NAME = srvr.LOGICAL_NAME) as HDD,
        srvr.ASSIGNMENT as GROUP_SOPR_SERVER,
        srvr.sb_responsibility_wg_name as GROUP_VLAD_SERVER,
        srvr.sb_administrator_group as GROUP_ADM_SERVER,
        srvr.sb_admin_group2_name as GROUP_SOPR_OS,
        -- srvr.sb_admin_group2_ID as "Сопр. ОС",
        srvr.SB_BELONGS_PCI_DSS,
        srvr.TPS_PLACEMENT,
        srvr.SB_SERVROOM,
        srvr.SB_COORD_RACK,
        srvr.TPS_SERVER_MODEL,
        REPLACE(REPLACE(srvr.TPS_COMMENTS1, CHR(13),' '),CHR(10),'') as TPS_COMMENTS1,
        REPLACE(REPLACE(srvr.TPS_COMMENTS2, CHR(13),' '),CHR(10),'') as TPS_COMMENTS2,     
        srvr.Created_BY_Date as DATE_CREATE,
        srvr.SYSMODTIME as DATE_CHANGE,
        srvr.HPC_STATUS as SERV_STATUS
    from DEVICE2M1 srvr
    where srvr.TYPE = 'server'
        and srvr.hpc_status != 'Out of Service'  
        and srvr.operating_system not like 'VMware ESX%'
        and srvr.SUBTYPE in ('Virtual', 'Physical', 'LPAR')
 """

# query to retrieve information resource objects
query_ir = """ 
    select ir.LOGICAL_NAME as KE,
        ir.Hpc_Device_Type_Rus AS KAT_IR,
        ir.subtype AS KE_TYPE,
        ir.tps_name AS NAME_IR,
        (select listagg(tps_support_groups,', ') within group (order by logical_name) 
            from 
            (select distinct m.tps_support_groups, admir.logical_name
                from device2a5 m
                left join device2m1 admir
                on m.logical_name=admir.logical_name
                where admir.Logical_Name = ir.logical_name 
                and ROWNUM <= 50))||';' as ADMIN_IR,
        (select listagg(email,'; ') within group (order by logical_name) 
            from (select distinct m.email, admir.logical_name
                    from device2a5 admir
                    left join contctsm1 m
                    on admir.tps_support_groups = m.full_name
                    where admir.logical_name = ir.logical_name 
                    and ROWNUM <= 50))||';' as EMAIL_IR,
        ir.tps_information_category AS KAT_INFO,
        ir.ASSIGNMENT AS KE_GROUP_SOPR_NAME,
        ir.sb_responsibility_wg_name AS KE_GROUP_VLAD_NAME,
        ir.sb_administrator_group AS SB_ADMINISTRATOR_GROUP,
        ir.environment as KE_ENVIROMENT,
        ir.HPC_STATUS as IR_STATUS,
        ir.CREATED_BY_DATE as RELATION_DATE, 
        ir."GROUP" as KE_GROUP_SOPR
    from DEVICE2M1 ir
    where ir.TYPE = 'infresource'
        and ir.hpc_status != 'Out of Service'
        and @IR_condition@
"""

# query to retrieve automated system objects
query_as = """ 
    select asts.LOGICAL_NAME as KE_AS,
        asts.tps_name as TPS_NAME,
        '' as AS_NAME,
        asts.hpc_device_type_rus as HPC_DEVICE_TYPE_RUS,
        asts.subtype as TYPE_AS,
        asts.tps_block as BLOCK,
        asts.SB_SERVICE_MAN_NAME as MIT,
        asts.tps_owner_id as TPS_OWNER_NAME, 
        (select m.J_PROVIDING_UNIT_NAME 
            from SBJITSERVICEM1 m 
            where m.logical_name = asts.logical_name
            ) AS DIT,
        asts.SB_SERVICE_LEVEL,
        substr(wg.sb_rc_local_coordinator_name, 1, instr(wg.sb_rsk_local_coordinator_name, '(', -1) - 2) as SB_PROBLEM_LOCAL_COORDINATOR,
        substr(wg.sb_rsk_local_coordinator_name, 1, instr(wg.sb_rsk_local_coordinator_name, '(', -1) - 2) as SB_RSK_LOCAL_COORDINATOR,
        asts.HPC_STATUS as HP_STATUS
    from DEVICE2M1 asts
    left join assignmentm1 wg 
        on asts.SB_ADMINISTRATOR_GROUP = wg.hpc_name_name 
    where (asts.TYPE = 'bizservice' or asts.HPC_DEVICE_TYPE_RUS = 'AS')
        and asts.HPC_STATUS != 'Out of Service'
"""

# query to retrieve cluster objects
query_cl = """ 
    select ci0.LOGICAL_NAME as ID_CLUSTER,
         ci0.TPS_NAME AS NAME_CLUSTER,
         ci0.SUBTYPE as TYPE_CLUSTER,
         ci0.ASSIGNMENT as GROUP_SOPR_CLU,
         ci0.HPC_DEVICE_TYPE_RUS as KAT_CLUSTER
    from  DEVICE2M1 ci0
    where ci0.type in ('cluster', 'sbvirtcluster')
        and ci0.HPC_STATUS != 'Out of Service'
"""

# query to retrieve DB objects
query_db_ex = """
    select ci.LOGICAL_NAME as KE_SUBD,
         ci.SUBTYPE as SUBTYPE_T
    from  DEVICE2M1 ci
    where ci.TYPE = 'dbmsinstance'
        and ci.HPC_STATUS != 'Out of Service'
"""

# query to retrieve composed information resource objects
query_colir = """ 
    select colir.logical_name AS KE_SIR,
         colir.hpc_device_type_rus AS KAT_SIR,
         colir.subtype AS TYPESIR,
         colir.tps_name AS NAMESIR
    from  DEVICE2M1 colir
    where colir.TYPE = 'collection'
        and colir.hpc_status != 'Out of Service'
"""

# query to retrieve stand objects
query_stnd = """ 
    select stnd.logical_name AS KE_STAND,
        stnd.hpc_device_type_rus AS KAT_STAND,
        stnd.subtype AS TYPE_STAND,
        stnd.tps_name AS NAME_STAND,
        stnd.sb_security_group AS GR_BEZ,
        (select listagg(tps_support_groups,', ') within group (order by logical_name) 
            from (select distinct m.tps_support_groups, adms.logical_name
                 from device2a5 m
                 left join device2m1 adms
                   on m.logical_name=adms.logical_name
                 where adms.Logical_Name = stnd.logical_name 
                   and ROWNUM <= 50))||';' as ADMIN_STAND,
        (select listagg(email,'; ') within group (order by logical_name) 
            from (select distinct m.email, adms.logical_name
                 from device2a5 adms
                 left join contctsm1 m
                   on adms.tps_support_groups = m.full_name
                 where adms.logical_name = stnd.logical_name and ROWNUM <= 50))||';' as EMAIL_STAND
    from  DEVICE2M1 stnd
    where stnd.type = 'environmenttype'
        and stnd.hpc_status != 'Out of Service'
"""


# function for getting data from the database into a dataframe
def dataframe_from_sql(query, connect, description=''):
    if len(description) > 0:
        print(description)
    start = time.time()

    df_result = pd.read_sql(query, con=connect)

    print(df_result.shape, end='', flush=True)
    print(f" : {time.time() - start:.0f} s")
    return df_result


# function for writing data from the dataframe to the database
def dataframe_to_sql(df, table_name, method, conn, description=''):
    if len(description) > 0:
        print(description, end='', flush=True)
    start = time.time()

    if method == 'replace':
        conn.execute('truncate table ' + table_name)

    dtype_dict = {}
    for col in list(df):
        if df[col].dtype == 'object':
            col_length = df[col].astype(str).str.len().max() + 1
            dtype_dict.update({col: types.VARCHAR(col_length)})
            engine.execute('alter table ' + table_name + ' modify "' + col + '" VARCHAR2(' + str(col_length) + ' CHAR)')

    df.to_sql(con=conn, name=table_name, if_exists='append', index=False, dtype=dtype_dict)

    print(f" : {time.time() - start:.0f} s")
    return


# function for writing data from a dataframe to a csv file
def dataframe_to_csv(df, file_name, method, description=''):
    if len(description) > 0:
      print(description, end='', flush=True)
    start = time.time()

    # today = time.strftime("%Y_%m_%d", time.localtime())
    df.to_csv(file_name, mode=method, encoding='utf-8', index=False, sep='`',
              date_format='%d.%m.%Y', header=True)

    # print(f" : {time.time() - start:.0f} s")
    return


# function for outputting log entries with timestamps
def log_write(description, time_start=0):
    if (time_start == 0):
        print(time.strftime("%H:%M:%S", time.localtime()), end='')
        print(f"  {description}")
        return time.time()
    else:
        print(time.strftime("%H:%M:%S", time.localtime()), end='')
        print(f" elapsed: {time.time() - time_start:.0f} s\n")
        return 0


# function for removing line breaks inside fields and empty characters on both sides of fields
def clear_data(df):
    df.replace("\n", " ", regex=True, inplace=True)
    df.replace("^\\s+", "", regex=True, inplace=True)
    df.replace("\\s+$", "", regex=True, inplace=True)

    return df


# function for downloading and saving data
def load_data(conn, ir_cond, postfix, debug=1):
    # debug = {1 : download data from SM; 2 : download data from SM and save to CSV; 3 : download data from CSV}
    global df_links
    global df_srv, df_ir, df_as, df_cl, df_db, df_colir, df_stnd
    global df_srv_ke, df_ir_ke, df_as_ke, df_cl_ke, df_db_ke, df_colir_ke, df_stnd_ke

    if debug == 1 or debug == 2:
        # configuration element links
        df_links = dataframe_from_sql(query_links, conn_read, " - request KE links")
        dataframe_to_csv(df_links, 'usp_links.csv', 'w', description='')

        # server characteristics
        df_srv = dataframe_from_sql(query_srv, conn_read, " - request servers")
        df_srv = clear_data(df_srv)
        df_srv['IP'] = df_srv.apply(lambda x: x['IP'][:-1] if (x['IP'].endswith(';')) else '0.0.0.0', axis=1)
        df_srv['IP'] = df_srv['IP'].replace(";", ",", regex=True)

        # information resource characteristics
        df_ir = dataframe_from_sql(query_ir.replace('@IR_condition@', ir_cond), conn_read, " - request IR")
        df_ir = clear_data(df_ir)

        # automated system characteristics
        df_as = dataframe_from_sql(query_as, conn_read, " - request AS")
        df_as = clear_data(df_as)

        # cluster characteristics
        df_cl = dataframe_from_sql(query_cl, conn_read, " - request clusters")
        df_cl = clear_data(df_cl)

        # DB characteristics
        df_db = dataframe_from_sql(query_db_ex, conn_read, " - request DB EX.")
        df_db = clear_data(df_db)

        # composed information resource characteristics
        df_colir = dataframe_from_sql(query_colir, conn_read, " - request COL.IR")
        df_colir = clear_data(df_colir)

        # stand characteristics
        df_stnd = dataframe_from_sql(query_stnd, conn_read, " - request stands")
        df_stnd = clear_data(df_stnd)

        if debug == 2:
            dataframe_to_csv(df_srv, 'usp_srv' + postfix + '.csv', 'w', description='')
            dataframe_to_csv(df_ir, 'usp_ir' + postfix + '.csv', 'w', description='')
            dataframe_to_csv(df_as, 'usp_as' + postfix + '.csv', 'w', description='')
            dataframe_to_csv(df_cl, 'usp_cl' + postfix + '.csv', 'w', description='')
            dataframe_to_csv(df_db, 'usp_db' + postfix + '.csv', 'w', description='')
            dataframe_to_csv(df_colir, 'usp_colir' + postfix + '.csv', 'w', description='')
            dataframe_to_csv(df_stnd, 'usp_stnd' + postfix + '.csv', 'w', description='')

    if debug == 3:
        df_srv = pd.read_csv('usp_srv' + postfix + '.csv', sep='`', low_memory=False)
        df_ir = pd.read_csv('usp_ir' + postfix + '.csv', sep='`', low_memory=False)
        df_as = pd.read_csv('usp_as' + postfix + '.csv', sep='`', low_memory=False)
        df_cl = pd.read_csv('usp_cl' + postfix + '.csv', sep='`', low_memory=False)
        df_db = pd.read_csv('usp_db' + postfix + '.csv', sep='`', low_memory=False)
        df_colir = pd.read_csv('usp_colir' + postfix + '.csv', sep='`', low_memory=False)
        df_stnd = pd.read_csv('usp_stnd' + postfix + '.csv', sep='`', low_memory=False)

    # filling in the column with the full domain name of the server
    df_srv['DNS_ALIAS_D'] = df_srv['DNS_ALIAS'] + '.' + df_srv['DOMAIN']

    # KE + AS name column fill
    df_as['AS_NAME'] = df_as['KE_AS'] + ' ' + df_as['TPS_NAME']

    # selection of dataframes with KE elements
    df_srv_ke = df_srv[['LOGICAL_NAME']].rename(columns={'LOGICAL_NAME': 'KE_SRV'})
    df_ir_ke = df_ir[['KE']].rename(columns={'KE': 'KE_IR'})
    df_as_ke = df_as[['KE_AS']]
    df_cl_ke = df_cl[['ID_CLUSTER']].rename(columns={'ID_CLUSTER': 'KE_CL'})
    df_db_ke = df_db[['KE_SUBD']]
    df_colir_ke = df_colir[['KE_SIR']]
    df_stnd_ke = df_stnd[['KE_STAND']]

    return


# function for creating a directed graph
def get_graph(direction):
    df_links = pd.read_csv('usp_links.csv', sep='`', names=['KE_PARENT', 'KE_CHILD'], low_memory=False)
    parent_col_name, child_col_name = list(df_links)
    if direction == 'p_to_c':
        return nx.from_pandas_edgelist(df_links, source=parent_col_name, target=child_col_name, create_using=nx.DiGraph())
    elif direction == 'c_to_p':
        return nx.from_pandas_edgelist(df_links, source=child_col_name, target=parent_col_name, create_using=nx.DiGraph())
    else:
        return None


# function for finding paths in the network
def network_search(df_from, graph, cutoff, direction):
    from_col_name, = list(df_from)
    df_res = pd.DataFrame()

    # loop through all starting nodes
    for index, item in df_from[from_col_name].items():
        # look for all shortest paths to the end nodes
        try:
            dict = nx.single_source_shortest_path(graph, item, cutoff=cutoff)
            # convert dictionary values from lists to strings
            dict = {index: ' '.join(value) for index, value in dict.items()}
            # form DataFrame from the obtained dictionary (keys - end nodes of paths, values - paths starting from the starting node)
            df_dict = pd.DataFrame.from_dict(dict, orient='index')
            # select the first path element into a separate column, the first and last path elements are discarded
            df_dict[from_col_name] = df_dict[0].apply(lambda x: x.split()[0])
            df_dict[0] = df_dict[0].apply(lambda x: ' '.join(x.split()[1:-1]))
            df_dict.rename(columns={0: from_col_name + '_' + direction}, inplace=True)
            # add the result to the final dataframe
            df_res = df_res.append(df_dict, sort=False)
        except:
            pass
    # print(f" - number of paths: {df_res.shape}")
    return df_res


# function for finding paths upwards in the network
def network_paths_up(df):
    graph = get_graph('c_to_p')
    return network_search(df, graph, 4, "UP")


# function to search for paths down in the network
def network_paths_down(df):
    graph = get_graph('p_to_c')
    return network_search(df, graph, 4, "DOWN")


# main function
def main(conn_read, conn_write, ir_query, table_postfix):
    global df_links
    global df_srv, df_ir, df_as, df_cl, df_db, df_colir, df_stnd
    global df_srv_ke, df_ir_ke, df_as_ke, df_cl_ke, df_db_ke, df_colir_ke, df_stnd_ke
    global p_to_c, c_to_p

    # link and object loading
    log_write("Link and object loading")

    df_ir = dataframe_from_sql(query_ir.replace('@IR_condition@', ir_query), conn_read, " - request IR")
    df_ir = clear_data(df_ir)
    dataframe_to_sql(df_ir[['KE', 'KE_TYPE']], 'x_ir' + table_postfix, 'replace', conn_write,
                     description=' - write x_ir' + table_postfix + ' table')

    load_data(conn_read, ir_query, table_postfix, 2)

    # splitting a dataframe with IR into parts for multiprocessing
    n_cores = multi.cpu_count()
    print(f" - number of processor cores: {n_cores}")
    df_split_ir = np.array_split(df_ir_ke, n_cores)

    # pathfinding from IR upwards
    log_write("Pathfinding from IR to AS")
    with multi.Pool(processes=n_cores) as pool:
        df_path = pd.concat(pool.map(network_paths_up, df_split_ir))
    pool.close()
    pool.terminate()

    # intersection with the AS dataframe
    df_ir_as = df_as_ke.merge(df_path, how='inner', left_on='KE_AS', right_index=True)
    print(df_ir_as.shape)
    # dataframe_to_sql(df_ir_as, 'x_ir_as' + table_postfix, 'replace', conn_write,
    #                  description=' - write x_ir_as' + table_postfix + ' table')

    # pathfinding from IR downwards
    log_write("Pathfinding from IR to Servers")
    with multi.Pool(processes=n_cores) as pool:
        df_path = pd.concat(pool.map(network_paths_down, df_split_ir))
    pool.close()
    pool.terminate()

    # overlap with the servers' dataframe
    df_ir_srv = df_srv_ke.merge(df_path, how='inner', left_on='KE_SRV', right_index=True)
    print(df_ir_srv.shape)

    # deletion of records with more than 4 IR per server
    log_write("Deletion of records with more than 4 IR per server")
    srv_ir_count = df_ir_srv[['KE_SRV', 'KE_IR']].groupby('KE_SRV')['KE_IR'].nunique()
    srv_ir_count = srv_ir_count[srv_ir_count <= 4]
    df_ir_srv = df_ir_srv.merge(pd.DataFrame(srv_ir_count), how='inner', left_on='KE_SRV', right_index=True)
    df_ir_srv = df_ir_srv.drop('KE_IR_y', axis=1).rename(columns={'KE_IR_x': 'KE_IR'})
    print(df_ir_srv.shape)
    # dataframe_to_sql(df_ir_srv, 'x_ir_srv' + table_postfix, 'replace', conn_write,
    #                  description=' - write x_ir_srv' + table_postfix + ' table')

    # paths consolidation
    log_write("Paths consolidation")
    df_srv_as = df_ir_srv.merge(df_ir_as, how='inner', left_on='KE_IR', right_on='KE_IR')
    df_srv_as = df_srv_as.reindex(columns=['KE_SRV', 'KE_IR_DOWN', 'KE_IR', 'KE_IR_UP', 'KE_AS'])
    print(df_srv_as.shape)
    # dataframe_to_sql(df_srv_as, 'x_srv_as' + table_postfix, 'replace', conn_write,
    #                  description=' - write x_srv_as' + table_postfix + ' table')
    # dataframe_to_csv(df_srv_as, 'x_srv_as' + table_postfix + '.csv', 'w', description='')

    # adding columns for other items
    df_srv_as.fillna('', inplace=True)
    df_srv_as.insert(1, 'KE_CL', '')
    df_srv_as.insert(2, 'KE_SUBD', '')
    df_srv_as.insert(6, 'KE_SIR', '')
    df_srv_as.insert(7, 'KE_STAND', '')

    # spreading other items across columns and deleting records with multiple AS
    log_write("Обработка промежуточных элементов")
    for i, row in df_srv_as.iterrows():
        for ke in row['KE_IR_DOWN'].split():
            if ke in df_cl_ke.values:
                row['KE_CL'] = ke
            elif ke in df_db_ke.values:
                row['KE_SUBD'] = ke
        for ke in row['KE_IR_UP'].split():
            if ke in df_as_ke.values:
                df_srv_as.drop(i, inplace=True)
            elif ke in df_colir_ke.values:
                row['KE_SIR'] = ke
            elif ke in df_stnd_ke.values:
                row['KE_STAND'] = ke

    print(df_srv_as.shape)
    # dataframe_to_sql(df_srv_as, 'x_srv_all_as' + table_postfix, 'replace', conn_write,
    #                  description=' - write x_srv_all_as' + table_postfix + ' table')
    # dataframe_to_csv(df_srv_as, 'x_srv_all_as' + table_postfix + '.csv', 'w', description='')

    # feature addition
    log_write("Feature addition")
    df_srv_as.drop(['KE_IR_DOWN', 'KE_IR_UP'], axis=1, inplace=True)
    df_srv_as = df_srv_as.merge(df_srv, how='left', left_on='KE_SRV', right_on='LOGICAL_NAME')
    df_srv_as = df_srv_as.merge(df_cl, how='left', left_on='KE_CL', right_on='ID_CLUSTER')
    df_srv_as = df_srv_as.merge(df_db, how='left', left_on='KE_SUBD', right_on='KE_SUBD')
    df_srv_as = df_srv_as.merge(df_ir, how='left', left_on='KE_IR', right_on='KE')
    df_srv_as = df_srv_as.merge(df_colir, how='left', left_on='KE_SIR', right_on='KE_SIR')
    df_srv_as = df_srv_as.merge(df_stnd, how='left', left_on='KE_STAND', right_on='KE_STAND')
    df_srv_as = df_srv_as.merge(df_as, how='left', left_on='KE_AS', right_on='KE_AS')

    # column ordering and saving
    df_srv_as.drop(['KE_SRV', 'KE_IR', 'KE_CL'], axis=1, inplace=True)
    df_srv_as = df_srv_as.reindex(columns=list(df_srv) + list(df_cl) + list(df_db) + list(df_ir) +
                                          list(df_colir) + list(df_stnd) + list(df_as))
    print(df_srv_as.shape)
    # dataframe_to_csv(df_srv_as, 'x_resource' + table_postfix + '.csv', 'w', description='')
    dataframe_to_sql(df_srv_as, 'x_resource' + table_postfix, 'replace', conn_write,
                     description=' - write x_resource' + table_postfix + ' table')

    return


if __name__ == "__main__":
    # getting IR from cmd-file environment variable
    try:
        os.environ['IR']
        ir = os.environ['IR']
        print(f"\nIR get from cmd environment: {ir}\n")
    # getting IR from the python command line
    except:
        ir = argv[1]
        print(f"\nIR get from command line: {ir}\n")

    # conditions for selection of IR
    IR_param_query = {
        'USP': """
             ir.subtype in (
                'Oracle AS',
                'WildFly',
                'Siebel',
                'DataPower',
                'IBM Websphere Portal Server',
                'Oracle iPlanet Web Server',
                'WebLogic Server',
                'SOWA',
                'IBM WSRP for Portal',
                'Apache Kafka',
                'DB2Portal',
                'WebSphere',
                'MQSeries',
                'BPM - IBM Process Server',
                'WebSphere Message Broker',
                'IBM Connections',
                'IBM InfoSphere',
                'Oracle HTTP Server',
                'ADAM',
                'NGINX',
                'IBM HTTP Server for Portal',
                'IBM FileNet Content Management',
                'WebLogic Server',
                'Oracle BI EE',
                'WebSphere eXtreme Scale',
                'Oracle WebTier'
            )
        """,
        'websphere': """
             ir.subtype in (
                'IBM HTTP Server for Portal',                       
                'IBM Websphere Portal Server',                      
                'IBM WSRP for Portal',                              
                'IBM Connections',                                  
                'IBM FileNet Content Management',                   
                'WebSphere eXtreme Scale',                          
                'WebSphere',       
            )
        """,
    }

    # tables for recording totals
    IR_param_table_postfix = {
        'USP': '',
        'openshift': '_openshift',
        'websphere': '_websphere',
    }

    # checking the correctness of the transmitted IR parameter
    try:
        ir_query = IR_param_query[ir]
        ir_table_postfix = IR_param_table_postfix[ir]
    except KeyError as e:
        raise ValueError('\nUndefined IR: {}\n'.format(e.args[0]))

    # database connection parameters
    ora_sid = ''
    ora_host = ''
    ora_port = ''
    username = ''
    password = ''

    ora_sid_2 = ''
    ora_host_2 = ''
    ora_port_2 = ''
    username_2 = ''
    password_2 = ''

    # database connection to read data
    dsn_tns = cx_Oracle.makedsn(ora_host, ora_port, ora_sid)
    conn_read = cx_Oracle.connect(username, password, dsn_tns)

    # database connection for data recording
    dsn_tns_2 = cx_Oracle.makedsn(ora_host_2, ora_port_2, ora_sid_2)
    cstr = 'oracle+cx_oracle://{user}:{passwd}@{sid}'.format(
      user=username_2,
      passwd=password_2,
      sid=dsn_tns_2
    )
    engine = create_engine(cstr) 
    conn_write = engine.connect()

    # program start
    main(conn_read, conn_write, ir_query, ir_table_postfix)
