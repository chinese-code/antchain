#!/bin/bash

# 测试运行脚本

echo "Running tests for datastream package..."

# 运行基本测试
echo "Running basic tests..."
python -m pytest tests/ -v

# 运行测试并生成覆盖率报告
echo "Generating coverage report..."
python -m pytest tests/ --cov=datastream --cov-report=term-missing --cov-report=html

echo "Tests completed. HTML coverage report is available in htmlcov/ directory."