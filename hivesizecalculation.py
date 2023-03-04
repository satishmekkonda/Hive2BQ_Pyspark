def fetchHiveTableSize(self):
    try :
        cu=ConnectionUtility()
        table_names=config.get(Constants.COMMON_SECTION, Constants.DB_TABLE).split(',')
        dataset=config.get(Constants.COMMON_SECTION, Constants.SOURCE_DB)
        conn_url=config.get(Constants.COMMON_SECTION, Constants.HIVE_CONN_URL)
        conn = cu.fetchHiveConnection(conn_url)
        if table_names[0]=='ALL':
            table_names=cu.fetchTableList(dataset,conn)
        tableSize = []
        total_table_counts=0
        for table in table_names:
            df=pd.read_sql(f'describe formatted {dataset}.{table}',conn)
            for i in range(len(df)):
                if  isinstance(df.iloc[i,1], str) and df.iloc[i,1].strip()=='totalSize':
                    total_table_counts += 1
                    size = int(df.iloc[i,2].strip()) *  0.000000001
                    logger.info(f'TabeName :::: {table}   TotalSize :: {size} GB')
                    tableSize.append(size)

        final_sum = sum(tableSize)

        logger.info(f"TotalSize of {total_table_counts} tables = {final_sum} GB")

    except Exception as e:
        logger.error(f'(type(e).__name__) Exception occured')
        raise e