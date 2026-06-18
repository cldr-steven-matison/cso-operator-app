"""Kafka helpers using aiokafka — topic stats, live tail, peek, produce."""

import asyncio
import base64
from typing import AsyncIterator

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
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


async def list_all_topics() -> list[dict]:
    """All topics with depth (end-offset sum)."""
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP)
    consumer = AIOKafkaConsumer(bootstrap_servers=settings.KAFKA_BOOTSTRAP)
    out: list[dict] = []
    try:
        await admin.start()
        await consumer.start()
        names = sorted(await admin.list_topics())
        for name in names:
            if name.startswith("__"):
                continue
            try:
                meta = await admin.describe_topics([name])
                info = meta[0] if meta else {}
                partitions = [
                    TopicPartition(name, p["partition"])
                    for p in info.get("partitions", [])
                ]
                if not partitions:
                    out.append({"topic": name, "partitions": 0, "depth": 0})
                    continue
                ends = await consumer.end_offsets(partitions)
                begins = await consumer.beginning_offsets(partitions)
                depth = sum(ends[p] - begins[p] for p in partitions)
                out.append(
                    {"topic": name, "partitions": len(partitions), "depth": depth}
                )
            except Exception:
                out.append({"topic": name, "partitions": 0, "depth": 0})
    finally:
        await consumer.stop()
        await admin.close()
    return out


async def produce(topic: str, body: bytes, headers: list[tuple[str, bytes]] | None = None) -> dict:
    """Publish a single message to a topic. Used by /api/ingest/* when no
    NiFi ListenHTTP URL is configured."""
    producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP)
    try:
        await producer.start()
        meta = await producer.send_and_wait(topic, value=body, headers=headers or [])
        return {"topic": meta.topic, "partition": meta.partition, "offset": meta.offset}
    finally:
        await producer.stop()


def _decode_payload(value: bytes | None) -> tuple[str, str | None]:
    """Return (text_preview, b64_or_None). Falls back to base64 when bytes
    aren't valid UTF-8 so binary topics (`new_audio`) are still inspectable."""
    if not value:
        return "", None
    try:
        text = value.decode("utf-8")
        return text[:1000], None
    except UnicodeDecodeError:
        preview = value.decode("utf-8", errors="replace")[:1000]
        return preview, base64.b64encode(value).decode("ascii")


async def peek(topic: str, limit: int = 10) -> list[dict]:
    """Return the latest `limit` messages on `topic`, newest-first.

    No consumer group → no offset commits. Each partition is rewound by
    `limit` from its end offset, then we drain briefly with getmany() and
    re-sort by (timestamp, offset) so we don't over-fetch on hot topics.
    """
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
        group_id=None,
    )
    await consumer.start()
    try:
        parts = consumer.partitions_for_topic(topic)
        if not parts:
            # partitions_for_topic can return None before the first metadata
            # refresh — force a metadata fetch and retry.
            await consumer._client.force_metadata_update()
            parts = consumer.partitions_for_topic(topic) or set()
        if not parts:
            return []
        tps = [TopicPartition(topic, p) for p in parts]
        consumer.assign(tps)
        ends = await consumer.end_offsets(tps)
        begins = await consumer.beginning_offsets(tps)
        for tp in tps:
            start = max(begins[tp], ends[tp] - limit)
            consumer.seek(tp, start)

        # Drain available records up to limit*partitions, capped by 2s wall.
        collected: list = []
        target = limit * max(1, len(tps))
        deadline = asyncio.get_event_loop().time() + 2.0
        while len(collected) < target:
            remaining_ms = int(max(0, deadline - asyncio.get_event_loop().time()) * 1000)
            if remaining_ms <= 0:
                break
            batch = await consumer.getmany(timeout_ms=min(500, remaining_ms))
            if not batch:
                break
            for recs in batch.values():
                collected.extend(recs)

        collected.sort(key=lambda m: (m.timestamp or 0, m.offset), reverse=True)
        out: list[dict] = []
        for m in collected[:limit]:
            preview, b64 = _decode_payload(m.value)
            entry = {
                "topic": m.topic,
                "partition": m.partition,
                "offset": m.offset,
                "ts": m.timestamp,
                "size": len(m.value or b""),
                "payload": preview,
            }
            if b64 is not None:
                entry["payload_b64"] = b64
            out.append(entry)
        return out
    finally:
        await consumer.stop()


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
