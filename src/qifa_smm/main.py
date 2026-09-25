from __future__ import annotations
import asyncio
import logging
import signal
from .config import get_settings
from .security import configure_secret_logging
from .db import Database
from .google_workspace import GoogleWorkspace, SheetRepository
from .jobs import JobRunner
from .llm import LLMClient
from .pipelines import PipelineService
from .publisher import TelegramPublisher
from .telegram import TelegramAPI, TelegramController


async def async_main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    configure_secret_logging(settings.telegram_control_bot_token, settings.telegram_publish_bot_token, settings.llm_api_key)
    log = logging.getLogger(__name__)
    if not settings.telegram_allowed_user_ids:
        raise ValueError("TELEGRAM_ALLOWED_USER_IDS is empty")
    database = Database(settings.database_path)
    await database.initialize()
    workspace = GoogleWorkspace(
        str(settings.google_oauth_token_file), settings.google_spreadsheet_id
    )
    sheets = SheetRepository(workspace)
    llm = LLMClient(
        settings.llm_base_url,
        settings.llm_api_key,
        settings.llm_model,
        mode=settings.llm_mode,
    )
    publisher = TelegramPublisher(
        settings.telegram_publish_bot_token,
        settings.telegram_test_channel_id,
    )
    pipelines = PipelineService(settings, database, workspace, sheets, llm, publisher)
    telegram_api = TelegramAPI(
        settings.telegram_control_bot_token, settings.telegram_long_poll_seconds
    )
    controller = TelegramController(settings, telegram_api, database, pipelines)

    async def notify(chat_id: int, text: str) -> None:
        await telegram_api.send_message(chat_id, text)

    runner = JobRunner(
        database,
        pipelines.handlers(),
        notify,
        poll_seconds=settings.job_poll_seconds,
    )
    tasks = [asyncio.create_task(controller.run(), name="telegram-poller")]
    tasks.extend(
        asyncio.create_task(runner.run(f"worker-{index + 1}"), name=f"worker-{index + 1}")
        for index in range(settings.job_workers)
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    log.info("QIFA SMM service started with %d worker(s)", settings.job_workers)
    try:
        wait_stop = asyncio.create_task(stop.wait(), name="shutdown-signal")
        done, _ = await asyncio.wait([*tasks, wait_stop], return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task is not wait_stop:
                task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await telegram_api.close()
        await publisher.close()
        log.info("QIFA SMM service stopped")


def run() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    run()
