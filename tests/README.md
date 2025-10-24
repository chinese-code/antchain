# 测试套件说明

本项目使用pytest作为测试框架，包含完整的单元测试套件，旨在达到尽可能高的代码覆盖率。

## 测试结构

```
tests/
├── __init__.py
├── conftest.py          # pytest配置文件
├── pytest.ini           # pytest配置文件
├── test_core.py         # 核心模块测试
├── test_strategies.py   # 策略模块测试
└── test_utils.py        # 工具模块测试
```

## 测试覆盖率

当前测试覆盖率报告：

```
Name                       Stmts   Miss  Cover   Missing
--------------------------------------------------------
datastream/__init__.py         4      0   100%
datastream/core.py           114     26    77%   109-123, 129, 134, 169-183, 189, 194
datastream/strategies.py     129     16    88%   33, 117, 133-141, 179, 195-203, 288
datastream/utils.py           25      3    88%   21, 36-37
--------------------------------------------------------
TOTAL                        272     45    83%
```

## 运行测试

### 基本测试运行

```bash
# 运行所有测试
python -m pytest tests/

# 运行测试并显示详细输出
python -m pytest tests/ -v

# 运行测试并生成覆盖率报告
python -m pytest tests/ --cov=datastream --cov-report=term-missing
```

### 生成HTML覆盖率报告

```bash
# 生成HTML格式的覆盖率报告
python -m pytest tests/ --cov=datastream --cov-report=html
```

生成的报告将保存在 `htmlcov/` 目录中，可以通过浏览器打开 `htmlcov/index.html` 查看详细的覆盖率信息。

## 测试内容

### test_core.py

测试核心模块的功能，包括：

- OPMode类的操作符重载功能
- Stream类的链式调用功能
- Start类的初始化功能
- 内置收集器（PEEK, LIST, SET, COUNT, TUPLE, FIRST, LAST）的功能

### test_strategies.py

测试各种数据处理策略：

- SingleItemStrategy（单条处理策略）
- BatchStrategy（批量处理策略）
- FilterStrategy（过滤处理策略）
- MergeStrategy（合并处理策略）
- LeftJoinStrategy（左连接处理策略）
- FullJoinStrategy（全连接处理策略）
- StrategyFactory（策略工厂）

### test_utils.py

测试工具函数：

- extract_batch_size（提取批处理大小）
- batch_process（批处理数据）

## 测试策略

1. **边界条件测试**：测试各种边界条件和异常情况
2. **功能测试**：验证每个函数和类的正常功能
3. **集成测试**：测试模块间的协作
4. **覆盖率测试**：确保尽可能高的代码覆盖率

## 依赖

测试依赖已在 `pyproject.toml` 中定义：

```toml
[tool.poetry.group.dev.dependencies]
pytest = "^7.0.0"
pytest-cov = "^4.0.0"
```

安装依赖：

```bash
# 如果使用poetry
poetry install

# 如果使用pip
pip install pytest pytest-cov
```