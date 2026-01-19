# 测试说明

本目录包含 TrendRadar 项目的测试脚本。

## 测试文件

### `test_incremental_detection.py`

测试增量检测功能（跨天支持），包括：

1. **存储管理器初始化**：验证存储后端是否正确初始化
2. **全局推送时间查询**：测试跨日期查询上次推送时间的功能
3. **跨天数据读取**：验证能否正确读取今天和昨天的数据
4. **增量检测逻辑**：测试26小时限制和基准时间逻辑

## 运行测试

### 方式一：直接运行

```bash
# Windows
python tests/test_incremental_detection.py

# Linux/Mac
python3 tests/test_incremental_detection.py
```

### 方式二：使用 pytest（如果安装了）

```bash
pytest tests/test_incremental_detection.py -v
```

## 测试说明

- 测试会检查 `output/news/` 目录下的数据库文件
- 如果数据库文件不存在或为空，测试仍会通过（这是正常的）
- 测试主要用于验证代码逻辑是否正确，不依赖实际数据

## 注意事项

- 确保项目根目录下有 `config/config.yaml` 和 `config/frequency_words.txt` 文件
- 测试会读取 `output/` 目录下的数据，不会修改数据
- Windows 环境下会自动处理控制台编码问题
