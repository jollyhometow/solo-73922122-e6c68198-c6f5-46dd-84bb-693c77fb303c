# 机构库 → 馆藏目录采集（含主题词规范词映射）

把机构库 OAI-PMH（`oai_dc`）的新增、修改、撤回同步为一份可导入离线发现系统的
馆藏目录 XML；并支持编目员上传“源词 → 规范词”CSV，对目录中的 `dc:subject`
做整表替换式精确映射。FastAPI + httpx + SQLite，附本地演示 OAI 源，
Docker Compose 一条命令启动。

## 快速启动

```bash
docker compose up --build -d
```

* 采集页：<http://localhost:8000/>（采集、下载、进度、错误、续采）
* 主题词映射页：<http://localhost:8000/mapping>（上传/应用/查看当前映射/下载）
* 本地演示 OAI-PMH 源：<http://localhost:8001/>（修改/撤回/凭据失效操作面板）
* 源端点：<http://localhost:8001/oai>（`ListRecords&metadataPrefix=oai_dc`）

```bash
docker compose logs -f catalog-web    # 观察采集中/发布失败/转换等日志
docker compose down                   # 停止（数据保留在命名卷）
docker compose down -v                # 连数据一起清空
```

无浏览器演示（容器内，覆盖采集、映射、并发与重算）：

```bash
docker compose exec catalog-web python -m scripts.demo_flow
```

## 主题词映射怎么用

1. 先在采集页完成至少一轮采集（应用范围是**最近一次成功发布的目录**）。
2. 打开 `/mapping`，上传 UTF-8 CSV：无表头，每行两列 `源词,规范词`，最多 200 条。
   上传是**整表替换**（保存为一个新版本，不与旧版本合并）；同一源词对应不同
   规范词会被拒绝；空文件（0 条）合法。
3. 点“应用选中版本”：后台从最近目录的**源值快照**重算，生成一份新目录并原子发布，
   与采集发布同构（同一张 `publications` 表、同一个下载入口）。
4. 换表：上传另一份 CSV 再应用，始终从源值重算，不会在旧结果上叠加；
   上传空表并应用即恢复全部源词。

映射语义：只对 `dc:subject` **逐值精确匹配，每个值最多替换一次**（即使规范词
恰好等于另一个源词也不级联），未命中值与其他 dc 元素原样保留；`records` 表
永远保存源端原始值。应用不包含尚未发布的采集记录，也**不推进采集水位**。

### 转换期间采集并发发布

转换在“生成前”有一个可配置暂停窗口（`MAPPING_APPLY_DELAY`，compose 默认 2 秒）。
窗口内采集可以照常完成并发布；转换提交时在同一事务里复查基准目录是否仍是最新
发布：若已被新采集取代，自动删除暂存文件、用最新快照重算，因此**迟到的转换
绝不会覆盖更新的源值或映射**。失败或切换中退出后重启：未提交的转换任务转
“失败”，孤儿文件启动清理，最近已提交的目录（内容 ↔ 映射版本 ↔ 水位三件套）
保持可用，可重新应用。

### 页面操作顺序（原有采集演示）

1. **首次全量**：选本轮截止日（默认今天）→ 开始采集。无水位时 `ListRecords`
   不带 `from`，收齐后生成目录并发布，水位 = 截止日。
2. **同日更新 / 撤回**：在 8001 演示源修改标题或撤回一条记录（源端 datestamp
   刷新为今天）→ 再次采集。窗口为 `from=上次成功截止日`（含当天），按
   `identifier` 更新或删除。未出现的记录不动，**不以缺席推断删除**。
3. **分页凭据失效**：演示源上点“下一次凭据请求失效”→ 采集端起续页时收到
   `badResumptionToken`，立即回到原窗口第一页重采，水位不推进、已入库记录不丢。
   页面的“凭据重置 N 次”和橙色提示会记录此事件。
4. **发布前后退出**：采集页“发布故障演练”选 `crash_after_publish` 或
   `crash_before_commit`，再跑一轮 → 进程退出。执行
   `docker compose restart catalog-web` 后，页面水位与下载文件仍是上一版完整结果；
   `crash_after_publish` 的轮次显示“发布失败·可重试”，点重试即可发布新版本，
   无需重采。
5. **转换前后退出**：映射页“转换故障演练”选 `crash_before_commit` /
   `crash_after_publish` 后应用映射 → 进程退出；重启后最近已提交目录不变、
   该转换任务显示 failed，重新应用即可。

## 行为设计

| 需求 | 实现 |
| --- | --- |
| 固定地址读取 oai_dc | `OAI_ENDPOINT` 固定，`ListRecords` + `resumptionToken` |
| 源按日标记变更、删除标记永久保留 | `records.identifier` 唯一，`deleted=1` 只置位不删除；重采/再版才改 |
| 首次全量，此后从上次成功截止日含当天重采 | `runs.mode=full/incremental`；`window_from=watermark`，`from/until` 均含当天 |
| 网页选择本轮截止日 | 首页日期表单 → `runs.window_until` |
| 按 identifier 更新或删除 | 每页记录 UPSERT；删除头写 tombstone |
| 不以缺席推断删除 | 本轮只处理返回记录，从不扫描本地记录做删除 |
| 同期只采一轮 | `runs` 上的部分唯一索引 + 应用层检查；未解决的轮次挡住新轮次 |
| 保存分页进度、断流/重启可续采 | 每页提交记录与 `resumption_token`（SQLite 事务）；启动把 `running` 转 `paused` |
| badResumptionToken 重采原窗口、不推进水位 | 收到该错误即重置页码/token 从原窗口第一页重采；水位只在发布时写 |
| 收齐后生成含全部有效记录的 XML | `records where deleted=0` 全量入目录，`<catalog watermark recordCount>` |
| 文件与水位一起发布 | 同一 `publications` 行保存文件路径+水位+sha256；页面/下载都只读该行 |
| 生成或发布失败保留旧目录 | 先构造 XML、写临时文件、原子 `os.replace`，最后落库；任一步失败旧行不动 |
| 发布中退出后重启，内容与水位对应同一完整结果 | 唯一文件名+原子改名；未落库的孤儿文件启动时清理；`publishing` 按有无发布记录补完成/转失败 |
| 采集中旧目录照常下载 | 下载只读最新 `publications` 行，与采集事务隔离 |
| 页面显示进度、错误、可续采 | 后台任务 + 2 秒自动刷新；进度条、错误码、继续采集/重试发布/放弃本轮 |
| 映射 CSV 最多 200 条、整表替换、冲突拒绝 | `mapping_versions`/`mapping_entries`；`parse_mapping_csv` 校验列数/空单元格/同源词冲突/上限；空表合法 |
| 同期只应用一份映射 | `mapping_jobs` 部分唯一索引，`running/applying` 未结束挡住新任务 |
| dc:subject 精确匹配只替换一次、未匹配保留 | `transform_subjects` 用单次 `dict.get`，不级联；只改 subject，源值永不改写 |
| 保存源值，换表从源值重算，空表恢复源词 | 每次发布把活动记录原始值快照写入 `publications.snapshot_json`；转换只从快照重算 |
| 应用范围为最近成功目录、不含未发布采集、不推进水位 | 转换只读最新 `publications` 的快照，新行沿用同一 `watermark`，不写 `runs` |
| 转换期间采集可发布且结果须包含新记录 | 提交事务内复查最新发布 id；基准变化则用新快照重算（`commit_mapping_publication`） |
| 迟到处理不覆盖更新的源值或映射 | 重算循环最多 5 次；每次都从当时最新快照 + 目标映射版本重新生成 |
| 下载内容、映射版本、水位对应同一完整结果 | 三者同处 `publications` 一行；XML 头含 `kind` 与 `mappingVersion`，下载响应含对应头 |
| 新映射与目录一起生效 | 暂存文件原子改名后，发布行与任务完成状态在同一事务提交 |
| 转换失败/切换中退出后重启保留最近已提交结果 | 启动 `recover_mapping_jobs_after_restart` 把未提交任务转 failed；孤儿清理；已提交行不变 |

## 测试

```bash
python3 -m pytest tests -q        # 或 docker compose exec catalog-web python -m pytest tests -q
```

覆盖：全量→同日增量、按 identifier 更新、撤回永久保留、缺席不删除、分页进度重启续采、
`badResumptionToken` 重采且水位不动、生成/落库失败保留旧目录、发布前后退出后的
“水位 ↔ 文件”一致性、目录哈希与记录数；以及映射 CSV 校验（200 条/冲突/空表/BOM）、
精确单次替换不级联、快照源值重算、空表恢复、未发布记录不入转换、并发采集自动重算、
转换失败与进程退出后的重启恢复、三件套（内容/映射版本/水位）一致、旧库 schema 迁移。

## 文件结构

```
app/           采集网页（main.py 路由、harvester.py 编排、oai.py 解析、catalog.py 发布、
               mapping.py 主题词映射编排、db.py SQLite）
source/        本地演示 OAI-PMH 源（同样 FastAPI + SQLite）
tests/         端到端行为测试（真实 ASGI 源，无外部依赖）
scripts/       demo_flow.py：无浏览器跑通采集+映射演示（容器内 python -m scripts.demo_flow）
Dockerfile / docker-compose.yml
```

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OAI_ENDPOINT` | `http://oai-source:8001/oai` | 固定 OAI-PMH 地址 |
| `CATALOG_DATA_DIR` | `/srv/data`（compose） | SQLite 与已发布目录 |
| `MAX_PAGES_PER_CALL` | 空（不限） | 单次后台采集页数上限，便于演示多次续采 |
| `PUBLISH_FAILURE_MODE` | `none` | 首次启动时预置一次性发布故障：`generate_error` / `commit_error` / `crash_before_commit` / `crash_after_publish` |
| `MAPPING_APPLY_DELAY` | `2`（compose） | 主题词转换生成前暂停秒数，演示转换期间采集并发发布与自动重算 |
| `SOURCE_DB_PATH` | `/srv/data/source.db` | 演示源 SQLite（`oai-source` 服务使用） |

演示源数据在 `source-data` 卷；采集端数据在 `catalog-data` 卷。
