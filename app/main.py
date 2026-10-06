"""FastAPI 入口。"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import api, payloads, repository as repo
from app.config import settings
from app.db import close_pool, init_db
from app.keyring import get_keyring
from app.objectstore import init_object_store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

log = logging.getLogger("webhook.api")

OWNER_ID = (
    f"api:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
)


async def cancelled_sweep_loop() -> None:
    """API 进程后台任务：周期清理已撤销事件的滞留投递（无需认领）。

    claim_delivery 只从 active 且非暂停的订阅认领；订阅暂停/停用后，事件
    撤销时租约已过期的 in_flight 投递不会再被任何认领事务访问，会滞留为
    in_flight。本巡检（与取消请求返回后的即时清理、Worker 巡检同源）直接
    把“事件已撤销”的 pending / 过期 in_flight 投递转 cancelled 并清除租约，
    不产生租约、不新增尝试；未过期的在途请求不动（成功仍可记 succeeded）。
    """
    from app.db import get_pool

    try:
        pool = get_pool()
    except RuntimeError:
        return
    interval = settings.cancel_sweep_interval_seconds
    log.info("撤销滞留投递后台清理任务已启动（间隔 %ss）", interval)
    while True:
        await asyncio.sleep(interval)
        try:
            async with pool.acquire() as conn:
                n = await repo.sweep_cancelled_deliveries(conn)
            if n:
                log.info("后台清理把 %s 条撤销滞留投递转为 cancelled", n)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("撤销滞留投递清理轮询失败")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    # 对象存储桶/目录就绪检查：不可用则启动失败（避免提交期才暴露）。
    await init_object_store()
    # 启动时加载本地密钥环：配置非法直接启动失败（避免运行期才暴露）；
    # 未配置则降级为只读（写入接口 503），详见 app/keyring.py。
    get_keyring()
    # 提交状态机后台恢复：崩溃/重启残留的“已登记/已上传未终结”提交由此续跑。
    recovery_task = asyncio.create_task(payloads.recovery_loop(OWNER_ID))
    # 撤销滞留投递后台清理：暂停/停用订阅下租约过期的在途投递无需认领转终态。
    sweep_task = asyncio.create_task(cancelled_sweep_loop())
    try:
        yield
    finally:
        sweep_task.cancel()
        recovery_task.cancel()
        for task in (sweep_task, recovery_task):
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        await close_pool()


app = FastAPI(
    title="可靠 Webhook 投递服务",
    version="1.0.0",
    description=(
        "事件幂等提交、内容寻址载荷对象存储、严格顺序投递、租约抢占、"
        "指数退避、死信重放、事件撤销（cancel）。交互文档见 /docs。"
    ),
    lifespan=lifespan,
)

app.include_router(api.router)


@app.get("/health", tags=["meta"])
async def health() -> dict[str, str]:
    return {"status": "ok"}
