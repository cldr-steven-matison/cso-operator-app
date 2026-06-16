"""Kafka helpers using aiokafka — topic stats and live tail."""

import asyncio
from typing import AsyncIterator

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

from config import settings

WATCHED = (settings.TOPIC_AUDIO, settings.TOPIC_DOCS)


async def topic_stats() -> list[dict]:
    """Per-topic: partition count, end offsets sum (treated as message count)."""
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP)
    consumer = AIOKafkaConsumer(bootstrap_servers=settings.KAFKA_BOOTSTRAP)
    out: list[dict] = []
    try:
        await admin.start()
        await consumer.start()
        meta = await admin.describe_topics(WATCHED)
        meta_by_name = {m["topic"]: m for m in meta}
        for name in WATCHED:
            info = meta_by_name.get(name)
            if not info or info.get("error_code"):
                out.append({"topic": name, "exists": False})
                continue
            partitions = [TopicPartition(name, p["partition"]) for p in info["partitions"]]
            ends = await consumer.end_offsets(partitions)
            beginnings = await consumer.beginning_offsets(partitions)
            depth = sum(ends[p] - beginnings[p] for p in partitions)
            out.append({
                "topic": name,
                "exists": True,
                "partitions": len(partitions),
                "depth": depth,
            })
    finally:
        await consumer.stop()
        await admin.close()
    return out


async def tail(topic: str) -> AsyncIterator[dict]:
    """Yield messages as they arrive, starting from the latest offset."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                payload = msg.value.decode("utf-8", errors="replace") if msg.value else ""
            except Exception:
                payload = f"<{len(msg.value or b'')} bytes>"
            yield {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "ts": msg.timestamp,
                "size": len(msg.value or b""),
                "payload": payload[:500],
            }
            await asyncio.sleep(0)
    finally:
        await consumer.stop()
