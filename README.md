# 天文瞬变事件警报与后续观测

只使用Python标准库和SQLite的模块化服务，默认端口`8338`。支持全天巡天来源、候选事件去重、事件对账、坐标与亮度测量合并、优先级计算、观测申请、望远镜排程、撤回、重分类、修正、观测队冲突、角色权限和审计。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：状态机、优先级、测量合并、事件对账（2角分/1天归并）和排程冲突约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等、事务和审计查询。
- `src/service.py`：用例编排、权限校验、事件对账、主记录认定、拆组和版本控制。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计（含越权拒绝留痕）。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8338
```

## 核心对象

`source`为巡天来源，`candidate`为瞬变候选，`event_group`为事件组，`telescope`为望远镜，`observation`为后续观测申请。

## 事件对账

- 同一来源事件号重复上报只入账一次；不同来源的坐标相差在2角分内、观测时刻相差一天内归入同一事件组，原始编号和测量保留。
- 来源数据变化后重新认定主记录；组内无其他有效上报时候选作废。
- 拆组使用乐观锁：两人同时拆组只让一方成功，另一方收到冲突并保留原组。
- 写入失败后可按上报号（`Idempotency-Key`）重试，候选与事件组在同一事务内写入，不留半组状态。
- 历史数据缺事件组时，管理员可`POST /api/backfill`补成单来源组；按条提交，中断后可续做。
- 越权确认或拆组返回403并写入审计。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`（确认`confirm`、拆组`split`等）
- `POST /api/backfill`（仅管理员）
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

轨道和观测计划使用简化的字符串时间窗比较，不包含真实天文历表、可见性预报、望远镜控制系统和观测数据存储。
