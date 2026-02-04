#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API 999.md — публикация объявлений о машинах на 999.md (черновики).
Запуск: uvicorn app:app --host 0.0.0.0 --port 8086
Или: python app.py
Авто-отправка на 999: задай AUTO_PUBLISH_999_ENABLED=1 (как в auto_send_tg).
"""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from publish_999md import router as publish_999md_router, start_auto_publish_999_thread


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_auto_publish_999_thread()
    yield


app = FastAPI(
    title="Publish 999.md",
    description="Публикация объявлений о машинах на 999.md (только черновики)",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(publish_999md_router)


@app.get("/")
def root():
    return {"service": "publish-999md", "docs": "/docs", "health": "/health"}


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("HTTP_PORT_999MD", "4040"))
    uvicorn.run(app, host=os.getenv("HTTP_HOST", "0.0.0.0"), port=port)
