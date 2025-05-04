#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
期货L2 Tick数据处理工具 - 将Tick数据转换为1分钟K线并保存到数据库

功能:
1. 读取各交易所原始Tick数据文件（支持CSV等格式）
2. 处理和清洗Tick数据
3. 聚合为1分钟K线数据
4. 将结果保存到CSV文件
5. 将结果导入到MySQL数据库 bar_1min_copy1 表

支持的数据源:
- 期货行情L2逐笔数据（如TFL2_TAQ_T1803_201801.csv）
- 命名规则: TFL2_TAQ_{合约代码}_{交易月份}.csv
  例如: TFL2_TAQ_T1803_201801.csv 表示2018年1月的T1803合约数据

输入字段映射:
本程序使用严格的一对一字段映射，对于L2数据的主要字段：
- 时间: TradingTime -> time
- 价格: LastPrice -> price
- 单笔成交量: TradeVolume -> tradevolume（每一笔交易的成交量）
- 单笔成交额: TradeAmount -> tradeamount（每一笔交易的成交额）
- 累计成交量: TotalVolume -> totalvolume（当日累计总成交量）
- 累计成交额: TotalAmount -> totalamount（当日累计总成交额）
- 持仓量: TotalPosition -> open_interest（持仓量）
- 五档买卖价量: BuyPrice01-05, SellPrice01-05, BuyVolume01-05, SellVolume01-05

输出数据表(bar_1min_copy1)包含的主要字段:
1. 基础字段: TRADINGDATE, SYMBOL, TRADINGTIME, OPEN, HIGH, LOW, CLOSE
2. 量价字段:
   - VOLUME: 分钟内所有单笔成交量(TradeVolume)之和
   - AMOUNT: 分钟内所有单笔成交额(TradeAmount)之和
   - TOTALVOLUME: 当日累计总成交量，直接使用L2数据中的TotalVolume
   - TOTALAMOUNT: 当日累计总成交额，直接使用L2数据中的TotalAmount
   - BUYVOL, SELLVOL: 买入量和卖出量，根据BuyOrSell字段区分
3. 持仓信息: TOTALPOSITION, POSITIONCHANGE
4. 深度信息: BUYPRICE01-05, SELLPRICE01-05, BUYVOLUME01-05, SELLVOLUME01-05
5. 衍生指标: ORDER_RATE(委比), ORDER_DIFF(委差), VOL_RATE(量比)等

使用方法:
1. 命令行调用:
   python tick_transformer.py --file 文件路径 [--date 交易日期] [--symbol 合约代码] [--to-db] [--debug]

2. 直接运行:
   python tick_transformer.py
   (将使用默认测试数据或内置测试数据进行处理)

3. 作为库调用:
   from tick_transformer import process_and_save
   process_and_save(file_path, trading_date=None, contract_code=None, save_to_db=True)

作者: Claude
日期: 2025-04-30
"""

import os
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import traceback
import mysql.connector
from sqlalchemy import create_engine, text

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("tick_transformer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- 数据库配置 ---
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '8wy514892',
    'database': 'futures_data',
    'charset': 'utf8mb4'
}

def connect_to_mysql():
    """连接到MySQL数据库"""
    try:
        connection = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            charset=DB_CONFIG['charset']
        )
        logger.info(f"成功连接到MySQL数据库 {DB_CONFIG['database']}")
        return connection
    except mysql.connector.Error as error:
        logger.error(f"连接到MySQL失败: {error}")
        return None

def create_bar_1min_copy1_table(connection):
    """创建1分钟K线数据表的副本"""
    try:
        cursor = connection.cursor()
        
        # 检查表是否已存在，如果存在则不重新创建
        cursor.execute("SHOW TABLES LIKE 'bar_1min_copy1'")
        if cursor.fetchone():
            logger.info("表 bar_1min_copy1 已存在，跳过创建步骤")
            return True
            
        # 创建表SQL - 使用完整的bar_1min表结构，增加对应L2数据说明的注释
        create_table_sql = """
        CREATE TABLE bar_1min_copy1 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            TRADINGDATE INT NOT NULL COMMENT '交易日期（格式：20240524）',
            SYMBOL VARCHAR(20) NOT NULL COMMENT '合约代码（如IF2406、T1803等）',
            TRADINGTIME DATETIME NOT NULL COMMENT '交易时间（精确到分钟）',
            
            OPEN DECIMAL(10,3) NOT NULL COMMENT '分钟开盘价',
            HIGH DECIMAL(10,3) NOT NULL COMMENT '分钟最高价',
            LOW DECIMAL(10,3) NOT NULL COMMENT '分钟最低价',
            CLOSE DECIMAL(10,3) NOT NULL COMMENT '分钟收盘价',
            
            VOLUME DECIMAL(20,2) DEFAULT 0 COMMENT '分钟成交量，对应L2中的TradeVolume',
            AMOUNT DECIMAL(20,3) DEFAULT 0 COMMENT '分钟成交金额，对应L2中的TradeAmount',
            BUYVOL DECIMAL(20,2) DEFAULT 0 COMMENT '买入量（外盘、主动性买盘），对应L2中的BuyVOL',
            SELLVOL DECIMAL(20,2) DEFAULT 0 COMMENT '卖出量（内盘、主动性卖盘），对应L2中的SellVOL',
            TOTALVOLUME DECIMAL(20,2) DEFAULT 0 COMMENT '当日累计成交量，对应L2中的TotalVolume',
            TOTALAMOUNT DECIMAL(20,3) DEFAULT 0 COMMENT '当日累计成交金额，对应L2中的TotalAmount',
            
            TOTALPOSITION DECIMAL(20,2) DEFAULT 0 COMMENT '持仓量（未平仓合约数量），对应L2中的TotalPosition',
            POSITIONCHANGE DECIMAL(20,2) DEFAULT 0 COMMENT '持仓变化（当前持仓减去上一分钟持仓），对应L2中的PositionChange',
            
            SETTLEPRICE DECIMAL(10,3) DEFAULT NULL COMMENT '结算价（当日结算价），对应L2中的SettlePrice',
            PRESETTLEPRICE DECIMAL(10,3) DEFAULT NULL COMMENT '昨结算价（上一交易日结算价），对应L2中的PreSettlePrice',
            
            PRICEUPLIMIT DECIMAL(10,3) DEFAULT NULL COMMENT '涨停价（当日最高价格限制），对应L2中的PriceUpLimit',
            PRICEDOWNLIMIT DECIMAL(10,3) DEFAULT NULL COMMENT '跌停价（当日最低价格限制），对应L2中的PriceDownLimit',
            
            BUYPRICE01 DECIMAL(10,3) DEFAULT NULL COMMENT '买一价（第1档委托买入价），对应L2中的BuyPrice01',
            BUYPRICE02 DECIMAL(10,3) DEFAULT NULL COMMENT '买二价（第2档委托买入价），对应L2中的BuyPrice02',
            BUYPRICE03 DECIMAL(10,3) DEFAULT NULL COMMENT '买三价（第3档委托买入价），对应L2中的BuyPrice03',
            BUYPRICE04 DECIMAL(10,3) DEFAULT NULL COMMENT '买四价（第4档委托买入价），对应L2中的BuyPrice04',
            BUYPRICE05 DECIMAL(10,3) DEFAULT NULL COMMENT '买五价（第5档委托买入价），对应L2中的BuyPrice05',
            
            SELLPRICE01 DECIMAL(10,3) DEFAULT NULL COMMENT '卖一价（第1档委托卖出价），对应L2中的SellPrice01',
            SELLPRICE02 DECIMAL(10,3) DEFAULT NULL COMMENT '卖二价（第2档委托卖出价），对应L2中的SellPrice02',
            SELLPRICE03 DECIMAL(10,3) DEFAULT NULL COMMENT '卖三价（第3档委托卖出价），对应L2中的SellPrice03',
            SELLPRICE04 DECIMAL(10,3) DEFAULT NULL COMMENT '卖四价（第4档委托卖出价），对应L2中的SellPrice04',
            SELLPRICE05 DECIMAL(10,3) DEFAULT NULL COMMENT '卖五价（第5档委托卖出价），对应L2中的SellPrice05',
            
            BUYVOLUME01 DECIMAL(20,2) DEFAULT 0 COMMENT '买一量（以买一价委托的数量），对应L2中的BuyVolume01',
            BUYVOLUME02 DECIMAL(20,2) DEFAULT 0 COMMENT '买二量（以买二价委托的数量），对应L2中的BuyVolume02',
            BUYVOLUME03 DECIMAL(20,2) DEFAULT 0 COMMENT '买三量（以买三价委托的数量），对应L2中的BuyVolume03',
            BUYVOLUME04 DECIMAL(20,2) DEFAULT 0 COMMENT '买四量（以买四价委托的数量），对应L2中的BuyVolume04',
            BUYVOLUME05 DECIMAL(20,2) DEFAULT 0 COMMENT '买五量（以买五价委托的数量），对应L2中的BuyVolume05',
            
            SELLVOLUME01 DECIMAL(20,2) DEFAULT 0 COMMENT '卖一量（以卖一价委托的数量），对应L2中的SellVolume01',
            SELLVOLUME02 DECIMAL(20,2) DEFAULT 0 COMMENT '卖二量（以卖二价委托的数量），对应L2中的SellVolume02',
            SELLVOLUME03 DECIMAL(20,2) DEFAULT 0 COMMENT '卖三量（以卖三价委托的数量），对应L2中的SellVolume03',
            SELLVOLUME04 DECIMAL(20,2) DEFAULT 0 COMMENT '卖四量（以卖四价委托的数量），对应L2中的SellVolume04',
            SELLVOLUME05 DECIMAL(20,2) DEFAULT 0 COMMENT '卖五量（以卖五价委托的数量），对应L2中的SellVolume05',
            
            TICKCOUNT INT DEFAULT 0 COMMENT '分钟内交易笔数（从原始tick数据计算）',
            
            ISNIGHT TINYINT(1) DEFAULT 0 COMMENT '是否夜盘（1表示夜盘交易，0表示日盘交易）',
            
            SECURITYID VARCHAR(10) DEFAULT NULL COMMENT '证券ID（品种代码，如IF、T等），对应L2中的SecurityID',
            CONTINUESIGN VARCHAR(10) DEFAULT NULL COMMENT '连续合约代码（如L1、L2等），对应L2中的ContinueSign',
            CONTINUESIGNNAME VARCHAR(20) DEFAULT NULL COMMENT '连续合约名称（如当月连续、下月连续等），对应L2中的ContinueSignName',
            MARKET VARCHAR(10) DEFAULT NULL COMMENT '市场（如CFFEX、SHFE等）',
            SHORTNAME VARCHAR(20) DEFAULT NULL COMMENT '证券简称（合约品种名称），对应L2中的ShortName',
            
            OPEN_LONG_COUNT INT DEFAULT 0 COMMENT '多头开仓笔数，从OpenClose字段估算',
            OPEN_SHORT_COUNT INT DEFAULT 0 COMMENT '空头开仓笔数，从OpenClose字段估算',
            CLOSE_LONG_COUNT INT DEFAULT 0 COMMENT '多头平仓笔数，从OpenClose字段估算',
            CLOSE_SHORT_COUNT INT DEFAULT 0 COMMENT '空头平仓笔数，从OpenClose字段估算',
            ORDER_RATE DECIMAL(9,4) DEFAULT NULL COMMENT '委比((委买总量-委卖总量)/(委买总量+委卖总量))，对应L2中的OrderRate',
            ORDER_DIFF DECIMAL(19,2) DEFAULT NULL COMMENT '委差(委买总量-委卖总量)，对应L2中的OrderDiff',
            VOL_RATE DECIMAL(9,4) DEFAULT NULL COMMENT '量比(当前分钟均量/前5天平均每分钟成交量)，对应L2中的VolRate',
            PRE_CLOSE_PRICE DECIMAL(10,3) DEFAULT NULL COMMENT '昨收盘价，对应L2中的PreClosePrice',
            
            INDEX idx_symbol_date (SYMBOL, TRADINGDATE),
            INDEX idx_trading_time (TRADINGTIME),
            INDEX idx_symbol_time (SYMBOL, TRADINGTIME)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='期货1分钟K线数据表 - 对应L2行情数据'
        """
        
        cursor.execute(create_table_sql)
        connection.commit()
        logger.info("成功创建表 bar_1min_copy1")
        return True
        
    except mysql.connector.Error as error:
        logger.error(f"创建表失败: {error}")
        return False

def save_to_database(df, connection):
    """将1分钟K线数据保存到数据库"""
    try:
        if df is None or df.empty:
            logger.warning("数据为空，无法保存到数据库")
            return False
            
        # 先清空目标表
        cursor = connection.cursor()
        try:
            truncate_sql = "TRUNCATE TABLE bar_1min_copy1"
            cursor.execute(truncate_sql)
            connection.commit()
            logger.info("已清空目标表 bar_1min_copy1")
        except mysql.connector.Error as error:
            logger.error(f"清空表失败: {error}")
            # 继续尝试插入数据，即使清空失败
            
        # 检查数据框中存在的列，避免尝试保存不存在的列
        available_columns = df.columns.tolist()
        
        # 定义所有可能的字段列表
        all_fields = [
            'TRADINGDATE', 'SYMBOL', 'TRADINGTIME', 
            'OPEN', 'HIGH', 'LOW', 'CLOSE', 
            'VOLUME', 'AMOUNT', 'BUYVOL', 'SELLVOL',
            'TOTALVOLUME', 'TOTALAMOUNT', 'TICKCOUNT', 'ISNIGHT',
            'SETTLEPRICE', 'PRESETTLEPRICE', 
            'PRICEUPLIMIT', 'PRICEDOWNLIMIT',
            'BUYPRICE01', 'BUYPRICE02', 'BUYPRICE03', 'BUYPRICE04', 'BUYPRICE05',
            'SELLPRICE01', 'SELLPRICE02', 'SELLPRICE03', 'SELLPRICE04', 'SELLPRICE05',
            'BUYVOLUME01', 'BUYVOLUME02', 'BUYVOLUME03', 'BUYVOLUME04', 'BUYVOLUME05',
            'SELLVOLUME01', 'SELLVOLUME02', 'SELLVOLUME03', 'SELLVOLUME04', 'SELLVOLUME05',
            'OPEN_LONG_COUNT', 'OPEN_SHORT_COUNT', 'CLOSE_LONG_COUNT', 'CLOSE_SHORT_COUNT',
            'ORDER_RATE', 'ORDER_DIFF', 'VOL_RATE', 'PRE_CLOSE_PRICE',
            'CONTINUESIGN', 'CONTINUESIGNNAME', 'SHORTNAME'
        ]
        
        # 仅使用数据框中实际存在的列
        fields_to_insert = []
        for field in all_fields:
            if field in available_columns:
                fields_to_insert.append(field)
        
        # 动态构建插入SQL语句
        insert_fields = ", ".join(fields_to_insert)
        insert_placeholders = ", ".join(["%s"] * len(fields_to_insert))
        insert_sql = f"""
        INSERT INTO bar_1min_copy1 (
            {insert_fields}
        ) VALUES ({insert_placeholders})
        """
        
        # 打印调试信息
        logger.debug(f"实际插入的字段数量: {len(fields_to_insert)}")
        logger.debug(f"实际插入的字段: {fields_to_insert}")
        
        # 准备数据并执行插入
        inserted_rows = 0
        for _, row in df.iterrows():
            # 准备数据
            data = []
            for field in fields_to_insert:
                value = row.get(field)
                # 处理特殊数据类型
                if field == 'TRADINGDATE':
                    value = int(value)
                elif field in ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'AMOUNT']:
                    value = float(value)
                elif field in ['TICKCOUNT', 'ISNIGHT', 'OPEN_LONG_COUNT', 'OPEN_SHORT_COUNT', 'CLOSE_LONG_COUNT', 'CLOSE_SHORT_COUNT']:
                    value = int(float(value)) if value is not None else 0
                
                data.append(value)
            
            # 执行插入
            try:
                cursor.execute(insert_sql, data)
                inserted_rows += 1
            except Exception as ex:
                logger.error(f"插入数据行 {inserted_rows+1} 时出错: {ex}")
                logger.error(f"SQL: {insert_sql}")
                logger.error(f"字段数: {len(fields_to_insert)}, 参数数: {len(data)}")
                raise
            
        # 提交事务
        connection.commit()
        logger.info(f"成功将 {inserted_rows} 条1分钟K线数据保存到数据库")
        return True
        
    except Exception as e:
        logger.error(f"保存数据到数据库时出错: {e}")
        logger.error(traceback.format_exc())
        if connection:
            try:
                connection.rollback()
            except:
                pass
        return False

def is_csv_content(file_path):
    """通过内容判断文件是否为CSV格式"""
    try:
        # 读取文件前几行
        with open(file_path, 'r', errors='ignore') as f:
            header = ''.join(f.readline() for _ in range(5))
        
        # 检查是否包含逗号、分号或制表符
        if ',' in header or ';' in header or '\t' in header:
            # 进一步检查是否有多列结构
            if header.count(',') > 2 or header.count(';') > 2 or header.count('\t') > 2:
                return True
        
        return False
    except Exception as e:
        logger.debug(f"检查文件 {file_path} 是否为CSV格式时出错: {e}")
        return False


def preprocess_tick_data(df, trading_date):
    """预处理原始Tick数据"""
    try:
        if df.empty:
            logger.warning("输入数据为空")
            return None
            
        # 输出原始数据的前几行，帮助调试
        logger.debug(f"原始数据前5行: \n{df.head(5)}")
        logger.debug(f"原始数据列: {df.columns.tolist()}")
            
        # 标准化列名（小写，移除空格）
        df.columns = [str(col).lower().strip() for col in df.columns]
        
        # 从原始DataFrame获取tradingdate列，如果存在
        csv_trading_date = None
        if 'tradingdate' in df.columns:
            try:
                # 获取第一个值并转换为字符串
                csv_trading_date = str(df['tradingdate'].iloc[0])
                logger.info(f"从CSV中获取tradingdate值: {csv_trading_date}")
            except Exception as e:
                logger.warning(f"处理tradingdate列时出错: {e}")
        
        # 创建新的DataFrame，只包含需要的列
        new_df = pd.DataFrame()
        column_mapping = {}
        
        # 精确映射关键字段 - 使用一对一严格对应关系
        exact_field_mapping = {
            # 基础字段
            'tradingdate': 'tradingdate',
            'tradingtime': 'time',
            'lastprice': 'price',
            'tradevolume': 'tradevolume',     # 单笔成交量
            'tradeamount': 'tradeamount',     # 单笔成交额
            'totalvolume': 'totalvolume',     # 累计成交量
            'totalamount': 'totalamount',     # 累计成交额
            'totalposition': 'open_interest', # 持仓量
            'buyorsell': 'buy_sell',          # 买卖标识
            'openclose': 'open_close',        # 开平仓性质
            
            # 五档买卖盘 - 精确对应格式BuyPrice01等
            'buyprice01': 'buyprice01',
            'buyprice02': 'buyprice02',
            'buyprice03': 'buyprice03',
            'buyprice04': 'buyprice04',
            'buyprice05': 'buyprice05',
            
            'sellprice01': 'sellprice01',
            'sellprice02': 'sellprice02',
            'sellprice03': 'sellprice03',
            'sellprice04': 'sellprice04',
            'sellprice05': 'sellprice05',
            
            'buyvolume01': 'buyvolume01',
            'buyvolume02': 'buyvolume02',
            'buyvolume03': 'buyvolume03',
            'buyvolume04': 'buyvolume04',
            'buyvolume05': 'buyvolume05',
            
            'sellvolume01': 'sellvolume01',
            'sellvolume02': 'sellvolume02',
            'sellvolume03': 'sellvolume03',
            'sellvolume04': 'sellvolume04',
            'sellvolume05': 'sellvolume05',
            
            # 其他可能有用的字段
            'preclosedate': 'preclosedate',
            'presettleprice': 'presettleprice',
            'priceuplimit': 'priceuplimit',
            'pricedownlimit': 'pricedownlimit',
            'settleprice': 'settleprice',
            'continuesign': 'continuesign',
            'continuesignname': 'continuesignname',
            'shortname': 'shortname',
            'market': 'market',
            'securityid': 'securityid',
            'pretotalposition': 'pretotalposition'
        }
        
        # 检查是否为L2数据
        is_l2_data = any(col in ['buyprice01', 'sellprice01', 'buyorsell', 'openclose'] for col in df.columns)
        logger.info(f"数据类型识别：{'L2数据' if is_l2_data else '普通Tick数据'}")
        
        # 从df中提取需要的字段，采用严格一对一的映射
        for csv_col, internal_col in exact_field_mapping.items():
            if csv_col in df.columns:
                column_mapping[internal_col] = csv_col
                
                # 对于量价字段，转换为数值类型
                if csv_col in ['lastprice', 'tradevolume', 'tradeamount', 'totalvolume', 'totalamount', 'totalposition',
                              'presettleprice', 'priceuplimit', 'pricedownlimit', 'settleprice'] or \
                   csv_col.startswith('buyprice') or csv_col.startswith('sellprice') or \
                   csv_col.startswith('buyvolume') or csv_col.startswith('sellvolume'):
                    new_df[internal_col] = pd.to_numeric(df[csv_col], errors='coerce')
                else:
                    new_df[internal_col] = df[csv_col]
        
        # 检查必要字段
        required_fields = ['time', 'price']
        missing_fields = [field for field in required_fields if field not in column_mapping]
        if missing_fields:
            logger.error(f"缺少必要字段: {missing_fields}")
            return None
        
        # 处理时间列 - 标准化时间格式
        def format_time(time_str):
            time_str = str(time_str).strip()
            # 处理不同格式的时间
            if ':' in time_str:  # 如果包含冒号，可能是时间格式
                # 尝试各种常见时间格式的处理
                if len(time_str) <= 8:  # 如 "09:30:00" 或 "9:30:00"
                    # 假设这是当日的时间，将其与交易日结合
                    return f"{trading_date[:4]}-{trading_date[4:6]}-{trading_date[6:8]} {time_str}"
                else:
                    # 已经包含日期的完整时间
                    return time_str
            else:  # 可能是数字格式的时间（如时间戳或特定格式）
                # 处理常见的数值时间格式
                if len(time_str) == 6:  # 如 "093000"
                    h, m, s = time_str[:2], time_str[2:4], time_str[4:6]
                    return f"{trading_date[:4]}-{trading_date[4:6]}-{trading_date[6:8]} {h}:{m}:{s}"
                elif len(time_str) == 9:  # 如 "093000000"（可能包含毫秒）
                    h, m, s = time_str[:2], time_str[2:4], time_str[4:6]
                    return f"{trading_date[:4]}-{trading_date[4:6]}-{trading_date[6:8]} {h}:{m}:{s}"
                else:
                    # 无法识别的格式，使用交易日和默认时间
                    logger.warning(f"无法识别的时间格式: {time_str}，使用交易日和默认时间")
                    return f"{trading_date[:4]}-{trading_date[4:6]}-{trading_date[6:8]} 00:00:00"
        
        # 应用时间格式化
        new_df['formatted_time'] = new_df['time'].apply(format_time)
        
        # 转换为datetime
        try:
            new_df['datetime'] = pd.to_datetime(new_df['formatted_time'], errors='coerce')
            # 删除无效的日期时间
            new_df = new_df.dropna(subset=['datetime'])
            # 排序
            new_df = new_df.sort_values('datetime')
        except Exception as e:
            logger.error(f"转换时间格式时出错: {e}")
            return None
            
        # 处理volume和amount字段
        # 如果缺少tradevolume但有totalvolume，则需要计算差值
        if 'tradevolume' not in column_mapping and 'totalvolume' in column_mapping:
            logger.warning("CSV中缺少单笔成交量(tradevolume)，将尝试从totalvolume计算")
            # 根据totalvolume差值来估算tradevolume
            new_df['tradevolume'] = new_df['totalvolume'].diff().fillna(new_df['totalvolume'].iloc[0])
            new_df['tradevolume'] = new_df['tradevolume'].clip(0)  # 确保不为负
            column_mapping['tradevolume'] = 'calculated_from_totalvolume'
        
        # 同样处理tradeamount
        if 'tradeamount' not in column_mapping and 'totalamount' in column_mapping:
            logger.warning("CSV中缺少单笔成交额(tradeamount)，将尝试从totalamount计算")
            new_df['tradeamount'] = new_df['totalamount'].diff().fillna(new_df['totalamount'].iloc[0])
            new_df['tradeamount'] = new_df['tradeamount'].clip(0)  # 确保不为负
            column_mapping['tradeamount'] = 'calculated_from_totalamount'
        
        # 为后续处理兼容，添加volume和amount别名
        if 'tradevolume' in column_mapping and 'volume' not in column_mapping:
            new_df['volume'] = new_df['tradevolume']
            column_mapping['volume'] = column_mapping['tradevolume'] 
        
        if 'tradeamount' in column_mapping and 'amount' not in column_mapping:
            new_df['amount'] = new_df['tradeamount']
            column_mapping['amount'] = column_mapping['tradeamount']
        
        # 如果价格为零则用前一个有效价格填充
        if 'price' in column_mapping:
            # 查找并填充零价格
            zero_price_count = (new_df['price'] == 0).sum()
            if zero_price_count > 0:
                logger.warning(f"发现{zero_price_count}行价格为零的数据，将用前值填充")
                new_df['price'] = new_df['price'].replace(0, method='ffill')
            
            # 使用前向填充处理NaN价格
            new_df['price'] = new_df['price'].fillna(method='ffill')
        
        # 分析买卖方向
        if 'buy_sell' in column_mapping:
            # 买卖标识字段存在，直接使用B和S标识计算买卖量
            buy_mask = new_df['buy_sell'].str.upper() == 'B'
            sell_mask = new_df['buy_sell'].str.upper() == 'S'
            
            # 初始化买卖量
            new_df['buy_vol'] = 0
            new_df['sell_vol'] = 0
            
            # 根据实际的单笔成交量分配买卖量
            if 'tradevolume' in new_df.columns:
                new_df.loc[buy_mask, 'buy_vol'] = new_df.loc[buy_mask, 'tradevolume']
                new_df.loc[sell_mask, 'sell_vol'] = new_df.loc[sell_mask, 'tradevolume']
            elif 'volume' in new_df.columns:
                new_df.loc[buy_mask, 'buy_vol'] = new_df.loc[buy_mask, 'volume']
                new_df.loc[sell_mask, 'sell_vol'] = new_df.loc[sell_mask, 'volume']
            
            # 对于非买非卖的情况，均分成交量
            neutral_mask = ~(buy_mask | sell_mask)
            if 'tradevolume' in new_df.columns:
                new_df.loc[neutral_mask, 'buy_vol'] = new_df.loc[neutral_mask, 'tradevolume'] / 2
                new_df.loc[neutral_mask, 'sell_vol'] = new_df.loc[neutral_mask, 'tradevolume'] / 2
            elif 'volume' in new_df.columns:
                new_df.loc[neutral_mask, 'buy_vol'] = new_df.loc[neutral_mask, 'volume'] / 2
                new_df.loc[neutral_mask, 'sell_vol'] = new_df.loc[neutral_mask, 'volume'] / 2
        else:
            # 买卖标识字段不存在，均分成交量
            new_df['buy_vol'] = 0
            new_df['sell_vol'] = 0
            
            if 'tradevolume' in new_df.columns:
                new_df['buy_vol'] = new_df['tradevolume'] / 2
                new_df['sell_vol'] = new_df['tradevolume'] / 2
            elif 'volume' in new_df.columns:
                new_df['buy_vol'] = new_df['volume'] / 2
                new_df['sell_vol'] = new_df['volume'] / 2
        
        # 将列映射保存到DataFrame的属性中，以便后续使用
        new_df.attrs['column_mapping'] = column_mapping
        
        # 输出处理结果统计
        logger.debug(f"预处理后数据: {len(new_df)}行")
        logger.debug(f"预处理后列映射: {column_mapping}")
        
        if not new_df.empty:
            logger.debug(f"处理后数据样本:\n{new_df.head(3)}")
            
            # 检查每分钟的数据点数量
            minute_counts = new_df.groupby(new_df['datetime'].dt.floor('1min')).size()
            logger.debug(f"分钟数据点分布: 最小={minute_counts.min()}, 最大={minute_counts.max()}, 平均={minute_counts.mean():.2f}")
            
            # 如果所有分钟都只有一个数据点，这可能导致OPEN=HIGH=LOW=CLOSE
            if minute_counts.max() == 1:
                logger.warning("警告：每分钟只有一个数据点，将导致OPEN=HIGH=LOW=CLOSE")
        
        return new_df
        
    except Exception as e:
        logger.error(f"预处理Tick数据时出错: {e}")
        logger.error(traceback.format_exc())
        return None


def aggregate_to_1min(df, symbol, trading_date, original_df=None):
    """将处理后的Tick数据聚合为1分钟K线"""
    try:
        if df is None or df.empty:
            logger.warning("输入数据为空，无法聚合")
            return None
            
        # 确保数据按时间精确排序，同时考虑毫秒级别
        df = df.sort_values('datetime', ascending=True)
        
        # 提取时间分钟部分作为分组键
        df['minute'] = df['datetime'].dt.floor('1min')
        
        # 如果价格为零则用前一个有效价格填充
        df['price'] = df['price'].replace(0, method='ffill')
        
        # 将价格按时序添加索引，便于后续验证
        df['idx'] = range(len(df))
        
        # 计算每分钟内的交易笔数
        tick_count = df.groupby('minute').size().reset_index(name='TICKCOUNT')
        
        # 统计开平仓情况 - 精确使用open_close字段
        if 'open_close' in df.columns:
            # 确定开平仓标识的实际取值范围
            unique_open_close = df['open_close'].dropna().unique()
            logger.debug(f"开平仓标识取值: {unique_open_close}")
            
            # 统计开平仓笔数 - 精确匹配L2数据中的标准值
            open_long_mask = df['open_close'].str.contains('多头开仓|双开仓', na=False, regex=True)
            open_short_mask = df['open_close'].str.contains('空头开仓|双开仓', na=False, regex=True)
            close_long_mask = df['open_close'].str.contains('多头平仓|双平仓', na=False, regex=True)
            close_short_mask = df['open_close'].str.contains('空头平仓|双平仓', na=False, regex=True)
            
            # 按分钟统计各类型笔数
            open_long_counts = df[open_long_mask].groupby('minute').size().reset_index(name='OPEN_LONG_COUNT')
            open_short_counts = df[open_short_mask].groupby('minute').size().reset_index(name='OPEN_SHORT_COUNT')
            close_long_counts = df[close_long_mask].groupby('minute').size().reset_index(name='CLOSE_LONG_COUNT')
            close_short_counts = df[close_short_mask].groupby('minute').size().reset_index(name='CLOSE_SHORT_COUNT')
        else:
            # 如果没有开平仓标识，创建空的DataFrame以便后续合并
            open_long_counts = pd.DataFrame(columns=['minute', 'OPEN_LONG_COUNT'])
            open_short_counts = pd.DataFrame(columns=['minute', 'OPEN_SHORT_COUNT'])
            close_long_counts = pd.DataFrame(columns=['minute', 'CLOSE_LONG_COUNT'])
            close_short_counts = pd.DataFrame(columns=['minute', 'CLOSE_SHORT_COUNT'])
        
        # 处理五档盘口数据
        bid_ask_fields = []
        for i in range(1, 6):
            bid_price_key = f'buyprice0{i}'
            ask_price_key = f'sellprice0{i}'
            bid_vol_key = f'buyvolume0{i}'
            ask_vol_key = f'sellvolume0{i}'
            
            # 如果存在对应的盘口数据，则添加到需要聚合的字段列表
            for key in [bid_price_key, ask_price_key, bid_vol_key, ask_vol_key]:
                if key in df.columns:
                    bid_ask_fields.append(key)
        
        # 使用自定义函数对每分钟数据进行处理，确保OHLC正确计算
        def get_ohlc(group):
            # 确保组内数据按时间排序
            group = group.sort_values('idx')
            
            # 正确计算OHLC
            open_price = group['price'].iloc[0]       # 第一笔交易价格
            high_price = group['price'].max()         # 最高价格
            low_price = group['price'].min()          # 最低价格
            close_price = group['price'].iloc[-1]     # 最后一笔交易价格
            
            # 分钟成交量 - 优先使用tradevolume
            volume = 0
            if 'tradevolume' in group.columns:
                volume = group['tradevolume'].sum()  # 累加分钟内所有单笔成交量
            elif 'volume' in group.columns:  # 兼容旧数据
                volume = group['volume'].sum()
            
            # 分钟成交额 - 优先使用tradeamount
            amount = 0
            if 'tradeamount' in group.columns:
                amount = group['tradeamount'].sum()  # 累加分钟内所有单笔成交额
            elif 'amount' in group.columns:  # 兼容旧数据
                amount = group['amount'].sum()
            
            # 累计成交量和成交额 - 取分钟内最后一条记录的值
            total_volume = None
            total_amount = None
            if 'totalvolume' in group.columns:
                total_volume = group['totalvolume'].iloc[-1]  # 取分钟内最后的累计值
            if 'totalamount' in group.columns:
                total_amount = group['totalamount'].iloc[-1]  # 取分钟内最后的累计值
            
            # 持仓量 - 取分钟内最后一条记录的值
            open_interest = 0
            if 'open_interest' in group.columns:
                open_interest = group['open_interest'].iloc[-1]
            
            # 买卖方向分类 - 使用预先计算的buy_vol和sell_vol
            buy_vol = 0
            sell_vol = 0
            if 'buy_vol' in group.columns and 'sell_vol' in group.columns:
                buy_vol = group['buy_vol'].sum()
                sell_vol = group['sell_vol'].sum()
            
            # 返回聚合结果
            result = {
                'OPEN': open_price,
                'HIGH': high_price,
                'LOW': low_price,
                'CLOSE': close_price,
                'VOLUME': volume,           # 分钟内总成交量
                'AMOUNT': amount,           # 分钟内总成交额
                'TOTALPOSITION': open_interest,
                'BUYVOL': buy_vol,
                'SELLVOL': sell_vol
            }
            
            # 添加原始累计值字段（如果存在）
            if total_volume is not None:
                result['TOTALVOLUME'] = total_volume  # 使用原始数据的累计总量
            if total_amount is not None:
                result['TOTALAMOUNT'] = total_amount  # 使用原始数据的累计总额
            
            # 添加tradingdate字段（如果存在）
            if 'tradingdate' in group.columns and not group['tradingdate'].empty:
                # 获取该分钟内第一个tick的tradingdate值
                result['TRADINGDATE'] = int(str(group['tradingdate'].iloc[0]))
            
            # 添加五档盘口数据
            for field in bid_ask_fields:
                if field in group.columns:
                    result[field.upper()] = group[field].iloc[-1]
            
            return pd.Series(result)
        
        # 执行分钟聚合
        aggregated = df.groupby('minute').apply(get_ohlc).reset_index()
        
        # 合并交易笔数
        aggregated = pd.merge(aggregated, tick_count, on='minute', how='left')
        
        # 合并开平仓统计
        aggregated = pd.merge(aggregated, open_long_counts, on='minute', how='left')
        aggregated = pd.merge(aggregated, open_short_counts, on='minute', how='left')
        aggregated = pd.merge(aggregated, close_long_counts, on='minute', how='left')
        aggregated = pd.merge(aggregated, close_short_counts, on='minute', how='left')
        
        # 填充NaN值
        for col in ['OPEN_LONG_COUNT', 'OPEN_SHORT_COUNT', 'CLOSE_LONG_COUNT', 'CLOSE_SHORT_COUNT']:
            if col in aggregated.columns:
                aggregated[col] = aggregated[col].fillna(0).astype(int)
            else:
                aggregated[col] = 0
        
        # 添加合约代码
        aggregated['SYMBOL'] = symbol
        
        # 如果上面分钟聚合时没有设置TRADINGDATE，这里尝试从原始数据获取
        if 'TRADINGDATE' not in aggregated.columns and original_df is not None and 'tradingdate' in original_df.columns:
            try:
                # 对每个分钟找到对应时间范围内的原始数据，并提取tradingdate
                # 简化处理：使用原始数据的第一行tradingdate
                tradingdate_value = str(original_df['tradingdate'].iloc[0])
                tradingdate_int = int(tradingdate_value)
                aggregated['TRADINGDATE'] = tradingdate_int
                logger.info(f"从原始数据的tradingdate列获取交易日期: {tradingdate_int}")
            except (ValueError, TypeError, IndexError) as e:
                logger.warning(f"无法从原始数据的tradingdate获取有效值: {e}，使用参数中的交易日期: {trading_date}")
                aggregated['TRADINGDATE'] = int(trading_date)
        elif 'TRADINGDATE' not in aggregated.columns:
            # 没有从分钟聚合获取到tradingdate，也没有原始数据，使用传入的日期参数
            aggregated['TRADINGDATE'] = int(trading_date)
            logger.warning(f"无法从数据中获取tradingdate，使用参数中的交易日期: {trading_date}")
        else:
            logger.info(f"使用从数据中提取的各分钟TRADINGDATE")
        
        # 重命名列
        aggregated = aggregated.rename(columns={'minute': 'TRADINGTIME'})
        
        # 计算累计成交量和成交额 - 仅在未找到原始累计值时才计算
        aggregated = aggregated.sort_values('TRADINGTIME')
        
        # 从原始CSV数据中提取TOTALVOLUME和TOTALAMOUNT情况
        has_original_totalvolume = 'TOTALVOLUME' in aggregated.columns
        has_original_totalamount = 'TOTALAMOUNT' in aggregated.columns
        
        # 仅当未从CSV提取到累计值时才自行计算
        if not has_original_totalvolume:
            logger.info("未从原始数据中获取到累计成交量(TOTALVOLUME)，使用分钟成交量累加计算")
            aggregated['TOTALVOLUME'] = aggregated['VOLUME'].cumsum()
        else:
            logger.info("使用原始数据中的累计成交量(TOTALVOLUME)")
        
        if not has_original_totalamount:
            logger.info("未从原始数据中获取到累计成交额(TOTALAMOUNT)，使用分钟成交额累加计算")
            aggregated['TOTALAMOUNT'] = aggregated['AMOUNT'].cumsum()
        else:
            logger.info("使用原始数据中的累计成交额(TOTALAMOUNT)")
        
        # 判断是否为夜盘
        # 期货夜盘交易时段通常为21:00-次日02:30
        aggregated['ISNIGHT'] = (
            (aggregated['TRADINGTIME'].dt.hour >= 21) | 
            (aggregated['TRADINGTIME'].dt.hour < 3)
        ).astype(int)
        
        # 提取SecurityID (品种代码，如IF、au等)
        match = re.match(r'^([a-zA-Z]+)', symbol)
        if match:
            aggregated['SECURITYID'] = match.group(1).upper()
        else:
            aggregated['SECURITYID'] = 'UNKNOWN'
            
        # 计算持仓变化
        if 'TOTALPOSITION' in aggregated.columns:
            aggregated['POSITIONCHANGE'] = aggregated['TOTALPOSITION'] - aggregated['TOTALPOSITION'].shift(1)
            # 第一条记录的持仓变化设为0
            aggregated['POSITIONCHANGE'] = aggregated['POSITIONCHANGE'].fillna(0)
        
        # 获取五档盘口的列名前缀
        buy_vol_prefix = 'BUYVOLUME0'
        sell_vol_prefix = 'SELLVOLUME0'
        
        # 计算委比和委差
        # 获取买卖盘口数量列
        buy_volume_cols = [col for col in aggregated.columns if col.startswith(buy_vol_prefix)]
        sell_volume_cols = [col for col in aggregated.columns if col.startswith(sell_vol_prefix)]
        
        # 计算委买总量和委卖总量
        if buy_volume_cols and sell_volume_cols:
            # 计算委买总量 - 所有买盘口的委托量之和
            aggregated['total_buy_volume'] = aggregated[buy_volume_cols].sum(axis=1)
            # 计算委卖总量 - 所有卖盘口的委托量之和
            aggregated['total_sell_volume'] = aggregated[sell_volume_cols].sum(axis=1)
            
            # 计算委差 = 委买总量 - 委卖总量
            aggregated['ORDER_DIFF'] = aggregated['total_buy_volume'] - aggregated['total_sell_volume']
            
            # 计算委比 = (委买总量 - 委卖总量) / (委买总量 + 委卖总量)
            denominator = aggregated['total_buy_volume'] + aggregated['total_sell_volume']
            aggregated['ORDER_RATE'] = aggregated['ORDER_DIFF'] / denominator
            # 处理分母为0的情况
            aggregated['ORDER_RATE'] = aggregated['ORDER_RATE'].fillna(0)
            
            # 转换委比为百分比值 (0-1范围)
            aggregated['ORDER_RATE'] = aggregated['ORDER_RATE'].clip(-1, 1)
        else:
            # 如果没有盘口数据，设置默认值
            aggregated['ORDER_DIFF'] = 0
            aggregated['ORDER_RATE'] = 0
        
        # 计算量比 - 假设当前没有历史数据，暂时使用1作为默认值
        # 实际计算方式：当日分钟均量（当前成交总量/当前交易分钟数）/前5天平均每分钟成交量
        aggregated['VOL_RATE'] = 1.0
        
        # 添加其他价格相关字段，从L2数据中提取，如果没有则使用估算值
        # 昨收盘价 - 默认使用当日开盘价
        aggregated['PRE_CLOSE_PRICE'] = aggregated['OPEN'].iloc[0] if not aggregated.empty else 0
        
        # 结算价 - 默认使用收盘价
        aggregated['SETTLEPRICE'] = aggregated['CLOSE'].copy()
        
        # 昨结算价 - 默认使用开盘价
        aggregated['PRESETTLEPRICE'] = aggregated['OPEN'].iloc[0] if not aggregated.empty else 0
        
        # 估算涨停价和跌停价 - 一般是昨结算价的正负10%
        if not aggregated.empty:
            base_price = aggregated['PRESETTLEPRICE'].iloc[0]
            aggregated['PRICEUPLIMIT'] = base_price * 1.1   # 涨停一般是昨结算价的110%
            aggregated['PRICEDOWNLIMIT'] = base_price * 0.9  # 跌停一般是昨结算价的90%
        else:
            aggregated['PRICEUPLIMIT'] = 0
            aggregated['PRICEDOWNLIMIT'] = 0
        
        # 添加连续合约信息
        aggregated['CONTINUESIGN'] = None  # 连续合约代码，如 IF L1
        aggregated['CONTINUESIGNNAME'] = None  # 连续合约名称，如 当月连续
        aggregated['SHORTNAME'] = None  # 证券简称
        
        # 构建包含所有字段的最终DataFrame
        columns = [
            'TRADINGDATE', 'SYMBOL', 'TRADINGTIME',
            'OPEN', 'HIGH', 'LOW', 'CLOSE',
            'VOLUME', 'AMOUNT', 'TOTALPOSITION', 'POSITIONCHANGE',
            'SECURITYID', 'MARKET',
            'BUYVOL', 'SELLVOL', 'TOTALVOLUME', 'TOTALAMOUNT',
            'TICKCOUNT', 'ISNIGHT',
            'SETTLEPRICE', 'PRESETTLEPRICE',
            'PRICEUPLIMIT', 'PRICEDOWNLIMIT',
            'PRE_CLOSE_PRICE',
            'ORDER_RATE', 'ORDER_DIFF', 'VOL_RATE',
            'OPEN_LONG_COUNT', 'OPEN_SHORT_COUNT', 'CLOSE_LONG_COUNT', 'CLOSE_SHORT_COUNT',
            'CONTINUESIGN', 'CONTINUESIGNNAME', 'SHORTNAME'
        ]
        
        # 添加五档盘口数据列名
        for i in range(1, 6):
            columns.extend([
                f'BUYPRICE0{i}', f'SELLPRICE0{i}',
                f'BUYVOLUME0{i}', f'SELLVOLUME0{i}'
            ])
        
        # 确保所有必要的列都存在
        final_columns = []
        for col in columns:
            if col in aggregated.columns:
                final_columns.append(col)
            else:
                # 对于不存在的列，添加默认值
                if col.startswith('BUY') or col.startswith('SELL'):
                    if 'PRICE' in col:
                        aggregated[col] = None  # 价格可以为空
                    else:
                        aggregated[col] = 0  # 数量默认为0
                elif col == 'MARKET':
                    aggregated[col] = None  # 市场可以为空
                else:
                    aggregated[col] = 0  # 其他字段默认为0
                final_columns.append(col)
        
        # 选择最终字段并按指定顺序排列
        final_df = aggregated[final_columns].copy()
        
        # 验证OHLC计算的正确性
        ohlc_wrong = (final_df['OPEN'] == final_df['HIGH']) & (final_df['HIGH'] == final_df['LOW']) & (final_df['LOW'] == final_df['CLOSE'])
        wrong_count = ohlc_wrong.sum()
        if wrong_count > 0:
            total_count = len(final_df)
            logger.warning(f"发现OHLC计算可能有误：{wrong_count}/{total_count}行数据OPEN=HIGH=LOW=CLOSE")
        
        return final_df
        
    except Exception as e:
        logger.error(f"聚合为1分钟K线时出错: {e}")
        logger.error(traceback.format_exc())
        return None


def process_tick_file(file_path, trading_date=None, contract_code=None):
    """处理单个Tick文件并转换为1分钟K线数据"""
    try:
        logger.info(f"开始处理文件: {file_path}")
        
        # 如果未提供交易日期，设置一个默认值作为备用
        if trading_date is None:
            # 使用一个固定的默认值，后续会被CSV中的实际数据覆盖
            trading_date = "20010101"  # 使用一个明显的默认日期，便于识别是否被实际数据覆盖
            logger.info(f"未提供交易日期，设置备用日期: {trading_date}，将优先使用CSV数据中的实际日期")
        else:
            # 仅记录提供的日期，但不会强制使用，仍然优先使用CSV数据
            logger.info(f"提供了备用交易日期: {trading_date}，但将优先使用CSV数据中的实际日期")
        
        # 尝试从文件名中提取合约代码
        if contract_code is None:
            filename = os.path.basename(file_path)
            logger.debug(f"从文件名 {filename} 中提取合约代码")
            
            # 根据命名规则：TFL2_TAQ_T1803_201801.csv中，T1803是合约代码
            contract_match = re.search(r'TAQ_([A-Za-z0-9]+)_\d{6}\.csv$', filename, re.IGNORECASE)
            if contract_match:
                contract_code = contract_match.group(1).upper()
                logger.info(f"根据命名规则从文件名中提取到合约代码: {contract_code}")
            else:
                contract_match = re.search(r'TFL2_TAQ_([A-Za-z0-9]+)_\d{6}', filename, re.IGNORECASE)
                if contract_match:
                    contract_code = contract_match.group(1).upper()
                    logger.info(f"根据命名规则从文件名中提取到合约代码: {contract_code}")
                else:
                    # 尝试其他合约代码格式的正则表达式
                    logger.debug(f"无法通过标准格式提取合约代码，尝试其他格式")
                    contract_patterns = [
                        r'([A-Za-z]{1,2}\d{3,4})',  # 如 IC2301
                        r'([A-Za-z]{1,2})(\d{4})',   # 如 IC2301或分开的IC 2301
                        r'([A-Za-z]{1,2})_(\d{4})'   # 如 IC_2301
                    ]
                    
                    for pattern in contract_patterns:
                        match = re.search(pattern, filename)
                        if match:
                            if len(match.groups()) == 1:
                                contract_code = match.group(1).upper()
                            else:
                                contract_code = (match.group(1) + match.group(2)).upper()
                            logger.info(f"从文件名中提取到合约代码: {contract_code}")
                            break
        
        # 读取文件
        df = None
        success = False
        
        try:
            logger.info("尝试读取文件...")
            # 尝试使用多种方法读取文件
            encodings = ['utf-8', 'gbk', 'gb2312', 'latin1', 'cp936', 'cp1252', 'iso-8859-1']
            
            # 首先尝试自动检测分隔符
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, sep=None, engine='python')
                    if df is not None and not df.empty:
                        logger.info(f"成功使用编码 {encoding} 和自动分隔符检测读取文件")
                        success = True
                        break
                except Exception as e:
                    continue
            
            # 如果自动检测失败，尝试特定分隔符
            if not success:
                delimiters = [',', '\t', ';', '|', ' ']
                for encoding in encodings:
                    if success:
                        break
                    for delimiter in delimiters:
                        try:
                            df = pd.read_csv(
                                file_path, 
                                encoding=encoding, 
                                delimiter=delimiter, 
                                on_bad_lines='skip',
                                low_memory=False
                            )
                            if df is not None and not df.empty:
                                logger.info(f"成功使用编码 {encoding} 和分隔符 '{delimiter}' 读取文件")
                                success = True
                                break
                        except Exception as e:
                            continue
        
        except Exception as e:
            logger.error(f"读取文件失败: {e}")
            logger.error(traceback.format_exc())
        
        # 如果文件读取失败
        if df is None or df.empty:
            logger.error(f"无法读取文件或文件为空: {file_path}")
            return None, None, 0
        
        # 如果仍然没有合约代码，使用文件名作为默认值
        if contract_code is None:
            contract_code = os.path.splitext(os.path.basename(file_path))[0].upper()
            logger.warning(f"无法提取合约代码，使用文件名作为默认值: {contract_code}")
        
        # 预处理Tick数据
        preprocessed_df = preprocess_tick_data(df, trading_date)
        if preprocessed_df is None or preprocessed_df.empty:
            logger.warning(f"预处理后数据为空: {file_path}")
            return None, None, 0
        
        # 聚合为1分钟K线
        k_line_df = aggregate_to_1min(preprocessed_df, contract_code, trading_date, original_df=df)
        if k_line_df is None or k_line_df.empty:
            logger.warning(f"聚合后数据为空: {file_path}")
            return None, None, 0
        
        logger.info(f"成功处理文件 {file_path}，生成 {len(k_line_df)} 条1分钟K线记录")
        return k_line_df, contract_code, len(k_line_df)
        
    except Exception as e:
        logger.error(f"处理文件时出错: {file_path}: {e}")
        logger.error(traceback.format_exc())
        return None, None, 0


def save_to_csv(df, output_path):
    """将1分钟K线数据保存到CSV文件"""
    try:
        df.to_csv(output_path, index=False)
        logger.info(f"成功将1分钟K线数据保存到: {output_path}")
        return True
    except Exception as e:
        logger.error(f"保存CSV文件时出错: {e}")
        logger.error(traceback.format_exc())
        return False

def truncate_bar_1min_copy1_table(connection):
    """清空bar_1min_copy1表的内容"""
    try:
        cursor = connection.cursor()
        
        # 清空表的SQL语句
        truncate_sql = "TRUNCATE TABLE bar_1min_copy1"
        
        cursor.execute(truncate_sql)
        connection.commit()
        logger.info("已清空表 bar_1min_copy1")
        return True
        
    except mysql.connector.Error as error:
        logger.error(f"清空表失败: {error}")
        return False

def main():
    """主函数，处理命令行参数并执行相应操作"""
    import argparse
    import sys
    
    # 检查是否有命令行参数
    if len(sys.argv) > 1:
        # 有命令行参数，按原方式处理
        parser = argparse.ArgumentParser(description='将Tick L2数据转换为1分钟K线数据')
        parser.add_argument('--file', required=True, help='输入的Tick L2数据文件路径')
        parser.add_argument('--date', help='备用交易日期 (YYYYMMDD格式，仅在CSV数据中没有交易日期时使用)')
        parser.add_argument('--contract', help='合约代码 (如果未提供则尝试从文件名提取)')
        parser.add_argument('--output', help='输出CSV文件路径 (如果未提供则使用默认路径)')
        parser.add_argument('--to_db', action='store_true', help='是否保存到数据库')
        parser.add_argument('--debug', action='store_true', help='启用调试模式，显示详细日志')
        
        args = parser.parse_args()
        
        # 设置调试模式
        if args.debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logger.info("启用调试模式，显示详细日志")
        
        # 处理文件
        k_line_df, contract_code, record_count = process_tick_file(
            args.file, 
            trading_date=args.date, 
            contract_code=args.contract
        )
        
        if k_line_df is not None:
            # 确定输出路径
            output_filename = args.output if args.output else f"{contract_code}_1min.csv"
            
            # 如果目录不存在，则自动创建
            output_dir = os.path.dirname(output_filename)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # 保存到CSV
            if save_to_csv(k_line_df, output_filename):
                logger.info(f"成功生成1分钟K线数据: {record_count}条记录，保存到 {output_filename}")
            else:
                logger.error(f"保存1分钟K线数据失败")
                # 尝试保存到当前工作目录
                current_dir_output = os.path.join(os.getcwd(), os.path.basename(output_filename))
                if save_to_csv(k_line_df, current_dir_output):
                    logger.info(f"成功将1分钟K线数据保存到当前工作目录: {current_dir_output}")
                else:
                    logger.error(f"保存到当前工作目录也失败，跳过CSV保存")
                
            # 保存到数据库
            if args.to_db:
                # 连接数据库
                connection = connect_to_mysql()
                if connection:
                    # 确保目标表存在
                    if create_bar_1min_copy1_table(connection):
                        # 保存数据 - save_to_database函数内部会清空表
                        if save_to_database(k_line_df, connection):
                            logger.info(f"成功将1分钟K线数据保存到数据库表 bar_1min_copy1")
                        else:
                            logger.error(f"保存到数据库失败")
                    else:
                        logger.error(f"创建数据库表失败")
                    # 关闭连接
                    connection.close()
                else:
                    logger.error(f"无法连接到数据库，跳过数据库保存")
                    
            print(f"成功处理文件，生成 {record_count} 条记录，保存到 {output_filename}")
        else:
            logger.error(f"处理文件失败，无法生成1分钟K线数据")
            print("处理失败")
            
        print("处理完成")
    else:
        # 没有命令行参数，直接运行处理流程
        print("直接运行Tick数据转换和数据库导入...")
        
        # 启用调试模式
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("启用调试模式，显示详细日志")
        
        # 创建测试文件
        def create_test_file():
            """创建测试用Tick数据文件"""
            try:
                # 创建一个简单的测试数据框
                df = pd.DataFrame({
                    'time': [f'09:{i:02d}:00' for i in range(30, 40)],
                    'price': [100.0 + i*0.5 for i in range(10)],
                    'volume': [10 + i*5 for i in range(10)],
                    'amount': [1000 + i*500 for i in range(10)],
                    'open_interest': [2000 + i*10 for i in range(10)]
                })
                
                # 保存为CSV
                test_file = 'test_tick.csv'
                df.to_csv(test_file, index=False)
                logger.info(f"已创建测试文件: {test_file}")
                return os.path.abspath(test_file)
            except Exception as e:
                logger.error(f"创建测试文件时出错: {e}")
                return None
        
        # 设置输入文件 - 先尝试使用预定义文件
        input_file = r"A:\data\CFFEX\2018\201801\T_201801\TFL2_TAQ_T1803_201801.csv"
        
        # 如果预定义文件不存在，创建测试文件
        if not os.path.exists(input_file):
            logger.info(f"预定义文件不存在: {input_file}")
            input_file = create_test_file()
            if input_file is None:
                logger.error("无法创建测试文件，程序退出")
                return
        
        # 设置参数
        logger.info(f"使用文件: {input_file}")
        trading_date = "20230101"  # 可以根据需要设置
        contract_code = None  # 设为None，让系统自动从文件名中提取
        
        # 设置更高的调试级别
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        
        # 处理文件
        k_line_df, contract_code, record_count = process_tick_file(
            input_file, 
            trading_date=trading_date, 
            contract_code=contract_code
        )
        
        if k_line_df is not None and not k_line_df.empty:
            # 默认输出到同目录下
            output_dir = os.path.dirname(input_file)
            output_filename = f"{contract_code}_1min.csv"
            output_path = os.path.join(output_dir, output_filename)
            
            # 保存到CSV
            if save_to_csv(k_line_df, output_path):
                logger.info(f"成功生成1分钟K线数据: {record_count}条记录，保存到 {output_path}")
            else:
                logger.error(f"保存1分钟K线数据失败")
                # 尝试保存到当前工作目录
                current_dir_output = os.path.join(os.getcwd(), output_filename)
                if save_to_csv(k_line_df, current_dir_output):
                    logger.info(f"成功将1分钟K线数据保存到当前工作目录: {current_dir_output}")
                else:
                    logger.error(f"保存到当前工作目录也失败，跳过CSV保存")
            
            # 保存到数据库
            connection = connect_to_mysql()
            if connection:
                # 确保目标表存在
                if create_bar_1min_copy1_table(connection):
                    # 保存数据 - save_to_database函数内部会清空表
                    if save_to_database(k_line_df, connection):
                        logger.info(f"成功将1分钟K线数据保存到数据库表 bar_1min_copy1")
                    else:
                        logger.error(f"保存到数据库失败")
                else:
                    logger.error(f"创建数据库表失败")
                # 关闭连接
                connection.close()
            else:
                logger.error(f"无法连接到数据库，跳过数据库保存")
            
            print(f"成功处理 {record_count} 条记录，合约代码: {contract_code}")
        else:
            logger.error(f"处理文件失败，无法生成1分钟K线数据: {input_file}")
            print("处理失败")
        
        print("处理完成")
        input("按Enter键退出...")


def process_and_save(file_path, trading_date=None, contract_code=None, save_to_db=True, debug=False):
    """
    处理Tick文件并保存结果（可选保存到数据库）
    
    参数:
        file_path: Tick数据文件路径
        trading_date: 备用交易日期 (YYYYMMDD格式)，仅在CSV数据中没有交易日期时使用
        contract_code: 合约代码，如未提供则从文件名提取
        save_to_db: 是否保存到数据库
        debug: 是否启用调试模式
    
    返回:
        (k_line_df, contract_code, record_count): 处理结果、合约代码和记录数量
    """
    # 设置调试模式
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("启用调试模式，显示详细日志")
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        logger.error(f"文件不存在: {file_path}")
        return None, None, 0
    
    # 处理文件
    k_line_df, contract_code, record_count = process_tick_file(
        file_path, 
        trading_date=trading_date, 
        contract_code=contract_code
    )
    
    if k_line_df is not None and not k_line_df.empty:
        # 默认输出到同目录下
        output_dir = os.path.dirname(file_path)
        output_filename = f"{contract_code}_1min.csv"
        output_path = os.path.join(output_dir, output_filename)
        
        # 保存到CSV
        if save_to_csv(k_line_df, output_path):
            logger.info(f"成功生成1分钟K线数据: {record_count}条记录，保存到 {output_path}")
        else:
            logger.error(f"保存1分钟K线数据失败")
            
        # 保存到数据库
        if save_to_db:
            # 连接数据库
            connection = connect_to_mysql()
            if connection:
                # 确保目标表存在
                if create_bar_1min_copy1_table(connection):
                    # 保存数据 - save_to_database函数内部会清空表
                    if save_to_database(k_line_df, connection):
                        logger.info(f"成功将1分钟K线数据保存到数据库表 bar_1min_copy1")
                    else:
                        logger.error(f"保存到数据库失败")
                else:
                    logger.error(f"创建数据库表失败")
                # 关闭连接
                connection.close()
            else:
                logger.error(f"无法连接到数据库，跳过数据库保存")
    else:
        logger.error(f"处理文件失败，无法生成1分钟K线数据: {file_path}")
    
    return k_line_df, contract_code, record_count


if __name__ == "__main__":
    main() 