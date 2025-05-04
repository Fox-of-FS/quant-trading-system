# 期货量化交易系统

本仓库包含期货量化交易系统的核心组件，主要功能包括:

## 核心组件

### tick_transformer.py

期货L2 Tick数据处理工具 - 将Tick数据转换为1分钟K线并保存到数据库

功能:
1. 读取各交易所原始Tick数据文件（支持CSV等格式）
2. 处理和清洗Tick数据
3. 聚合为1分钟K线数据
4. 将结果保存到CSV文件
5. 将结果导入到MySQL数据库 bar_1min_copy1 表

使用方法:
```bash
# 命令行调用
python tick_transformer.py --file 文件路径 [--date 交易日期] [--symbol 合约代码] [--to-db] [--debug]

# 或直接运行
python tick_transformer.py
```