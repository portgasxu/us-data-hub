# Git + GitHub 版本管理工作流

## 日常开发流程

### 1. 修改代码后推送更新

```bash
# 查看改动了什么
git status
git diff

# 添加到暂存区
git add -A          # 添加所有改动
# 或
git add <文件路径>   # 添加指定文件

# 提交
git commit -m "feat: 添加新的风控模块"

# 推送到 GitHub
git push
```

### 2. 从 GitHub 拉取更新

```bash
# 拉取并合并（推荐）
git pull

# 如果远程有更新，本地也有修改
git pull --rebase
```

### 3. 在另一台机器上同步

```bash
# 首次克隆
git clone https://github.com/portgasxu/us-data-hub.git

# 后续更新
cd us-data-hub && git pull
```

---

## 分支管理（可选，简单项目）

```bash
# 创建新功能分支
git checkout -b feature/risk-arbitrator

# 开发完成后合并到主分支
git checkout main
git merge feature/risk-arbitrator
git push
```

**简单项目直接用 main 分支也完全可以。**

---

## Commit 消息规范

```
类型: 简短描述

类型说明:
  feat:    新功能
  fix:     修复 bug
  docs:    文档更新
  refactor: 代码重构
  perf:    性能优化
  test:    测试相关
  chore:   构建/工具相关
```

**示例：**
```bash
git commit -m "feat: 添加订单冷却机制"
git commit -m "fix: 修复夏令时转换 bug"
git commit -m "docs: 更新 README 架构图"
git commit -m "perf: 优化数据采集并行度"
```

---

## 安全规则（重要！）

### 绝对不能提交的文件

- `.env`（含 API Key）
- `*.db` / `*.sqlite`（本地数据库）
- `logs/`（日志文件）
- `data/raw/` / `data/processed/`（运行时数据）
- `output/`（输出结果）

### 提交前检查

```bash
# 确认没有敏感文件
git status
git diff --cached | grep -i "sk-\|api_key\|secret"
```

`.gitignore` 已配置好上述排除规则。

---

## 让 AI 助手帮忙推送

告诉助手类似这样的话：

- "帮我提交最新的代码改动"
- "把刚修改的文件推送到 GitHub"
- "拉取 GitHub 上的最新代码"

助手会执行：
1. `git status` 查看改动
2. `git add -A` 添加文件
3. `git commit -m "描述"` 提交
4. `git push` 推送

---

## 常见问题

### Q: 推送到 GitHub 被拒绝怎么办？

```bash
# 远程有更新，先拉取
git pull --rebase
# 解决冲突后
git push
```

### Q: 提交错了怎么办？

```bash
# 撤销最后一次提交（保留改动）
git reset --soft HEAD~1

# 修改最后一次提交消息
git commit --amend -m "新的提交消息"
```

### Q: 不想提交某个文件的修改？

```bash
# 从暂存区移除（保留本地修改）
git reset HEAD <文件路径>
```

---

## 当前认证配置

GitHub Token 已配置在服务器的 Git remote URL 中。
如需更换 Token：

```bash
git remote set-url origin https://oauth2:新TOKEN@github.com/portgasxu/us-data-hub.git
```
