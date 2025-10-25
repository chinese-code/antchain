# 贡献指南

感谢您对AntChain项目的关注！我们欢迎各种形式的贡献。

## 开发环境设置

1. 克隆仓库：
   ```bash
   git clone https://github.com/tumingjian/ant-chain.git
   cd ant-chain
   ```

2. 创建虚拟环境：
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # 或者
   venv\Scripts\activate  # Windows
   ```

3. 安装开发依赖：
   ```bash
   pip install -e ".[dev]"
   ```

## 代码规范

### 代码风格

我们遵循PEP 8代码风格规范，使用以下工具进行代码格式化：

- **Black**: 代码格式化工具
- **isort**: 导入语句排序工具
- **flake8**: 代码风格检查工具

运行代码格式化：
```bash
black antchain/
isort antchain/
```

运行代码风格检查：
```bash
flake8 antchain/
```

### 类型检查

我们使用mypy进行静态类型检查：
```bash
mypy antchain/
```

### 提交信息规范

请遵循以下提交信息格式：
```
<type>(<scope>): <subject>

<body>

<footer>
```

类型包括：
- feat: 新功能
- fix: 修复bug
- docs: 文档更新
- style: 代码格式调整
- refactor: 代码重构
- test: 测试相关
- chore: 构建过程或辅助工具的变动

## 测试

### 运行测试

```bash
# 运行所有测试
python -m pytest

# 运行特定测试文件
python -m pytest tests/test_stream.py

# 运行测试并生成覆盖率报告
python -m pytest --cov=antchain --cov-report=html
```

### 编写测试

测试文件应放在`tests/`目录下，文件名以`test_`开头。使用unittest框架编写测试。

## Pull Request 流程

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 问题报告

如果您发现bug或有功能建议，请在GitHub上创建issue。

### Bug报告

请包含以下信息：
- Python版本
- AntChain版本
- 复现步骤
- 期望行为
- 实际行为
- 错误信息（如果有）

## 代码结构

```
antchain/
├── __init__.py          # 包初始化文件
├── element.py           # 数据流元素类
├── exceptions.py        # 异常定义
├── strategy.py          # 处理策略
├── stream.py            # 数据流核心类
├── utils.py             # 工具函数
└── validators.py        # 验证器
tests/
├── test_element.py      # Element类测试
├── test_exceptions.py   # 异常测试
├── test_strategy.py     # 策略测试
├── test_stream.py       # Stream类测试
├── test_utils.py        # 工具函数测试
├── test_validators.py   # 验证器测试
├── test_integration.py  # 集成测试
├── test_performance.py  # 性能测试
└── test_new_operators.py # 新操作符测试
example/
└── advanced_usage.py    # 高级使用示例
```

## 发布流程

1. 更新版本号在`pyproject.toml`和`__init__.py`中
2. 更新`CHANGELOG.md`
3. 创建发布标签
4. 构建并发布到PyPI

## 许可证

通过贡献代码，您同意您的贡献将遵循MIT许可证。