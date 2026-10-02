# 天文瞬变事件警报与后续观测

只使用Python标准库和SQLite的模块化服务，默认端口`8338`。支持全天巡天来源、候选事件去重、跨来源事件对账与事件组、坐标与亮度测量合并、优先级计算、主记录重认定、观测申请、望远镜排程、撤回、重分类、修正、观测队冲突、角色权限、乐观并发和审计。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：状态机、优先级、测量合并、事件组权限和排程冲突约束。
- `src/matching.py`：2角分坐标阈值与1天观测时间窗的跨来源匹配。
- `src/repository.py`：SQLite持久化、事务化对账、乐观锁、幂等、迁移补建和审计查询。
- `src/service.py`：用例编排、权限校验和版本控制。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8338
```

## 核心对象

`source`为巡天来源，`candidate`为瞬变候选（原始来源编号和测量均保留），`event_group`为对账后的事件组，`telescope`为望远镜，`observation`为后续观测申请。

## 事件对账

- 候选上报可携带稳定的`report_id`；也可用`Idempotency-Key`充当该上报号。同一上报号重试只返回已入账候选，不产生半组。
- 同一来源的`event_id`在有效候选中唯一；撤回后可重新上报。
- 不同来源的候选坐标相差不超过2角分、观测时刻相差不超过1天时自动归入同一`event_group`。
- 组内主候选按优先级重认定；最后一个有效候选撤回时事件组作废。
- 事件组支持`confirm`；`split`需要`candidate_ids`、`reason`和`expected_version`。并发拆组只有一方成功，另一方收到409且原组不变。
- 旧库初始化时自动把缺少事件组的历史候选补成单来源组；补建按批次提交，中断后再次启动会跳过已完成候选并继续。
- 越权的确认、拆组或手工建组返回403，并写入`*_denied`审计。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

轨道和观测计划使用简化的字符串时间窗比较，不包含真实天文历表、可见性预报、望远镜控制系统和观测数据存储。
