"""
Role-specific RabbitMQ worker services for Allocare.

The worker can run in three modes:
- audit_logger: stores all consumed events in the audit trail
- alert_system: generates alerts from critical events
- real_time_dashboard: maintains live dashboard metrics
"""

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import pika


BASE_DIR = Path(__file__).resolve().parent.parent
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
RABBITMQ_EXCHANGE = os.environ.get("RABBITMQ_EXCHANGE", "allocare.events")
DATABASE_PATH = os.environ.get("DATABASE_PATH", str(BASE_DIR / "data" / "allocare.db"))
CONSUMER_ROLE = os.environ.get("CONSUMER_ROLE", "audit_logger")
SYSTEM_NOTIFICATION_QUEUE = "allocare.system.notifications"

ROLES = {
    "audit_logger": {
        "queue": "allocare.audit.logger",
        "bindings": [
            "allocare.auth.events",
            "allocare.patient.admission",
            "allocare.bed.updates",
            "allocare.bed.capacity",
            "allocare.doctor.registration",
            "allocare.filesystem.events",
            "allocare.group.operations",
            "allocare.group.admin",
            SYSTEM_NOTIFICATION_QUEUE,
        ],
        "description": "Audit Logger",
    },
    "alert_system": {
        "queue": "allocare.alert.system",
        "bindings": [
            "allocare.patient.admission",
            "allocare.bed.capacity",
            "allocare.doctor.registration",
            "allocare.filesystem.events",
            "allocare.group.care.team",
            SYSTEM_NOTIFICATION_QUEUE,
        ],
        "description": "Alert System",
    },
    "real_time_dashboard": {
        "queue": "allocare.real.time.dashboard",
        "bindings": [
            "allocare.auth.events",
            "allocare.patient.admission",
            "allocare.bed.updates",
            "allocare.bed.capacity",
            "allocare.doctor.registration",
            "allocare.filesystem.events",
            "allocare.group.care.team",
            "allocare.group.operations",
            SYSTEM_NOTIFICATION_QUEUE,
        ],
        "description": "Real-Time Dashboard",
    },
}


def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_db_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS consumed_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                delivery_tag TEXT,
                ack_status TEXT DEFAULT 'acknowledged',
                consumed_at TEXT NOT NULL,
                consumed_by TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                source_data TEXT,
                is_resolved INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                resolved_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_system_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_size INTEGER,
                event_category TEXT,
                file_extension TEXT,
                processed_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dashboard_event_metrics (
                metric_key TEXT PRIMARY KEY,
                metric_value INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS group_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_name TEXT NOT NULL,
                routing_key TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                payload TEXT,
                created_by TEXT,
                created_at TEXT NOT NULL,
                consumed_at TEXT NOT NULL,
                consumed_by TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS system_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                notification_type TEXT NOT NULL,
                source TEXT NOT NULL,
                payload TEXT,
                consumed_at TEXT NOT NULL,
                consumed_by TEXT NOT NULL
            )
            """
        )


def store_consumed_event(queue_name: str, event_data: dict, consumer_role: str, delivery_tag: str = None) -> None:
    """Store consumed event in database with at-least-once delivery guarantee."""
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO consumed_events (
                queue_name,
                event_type,
                payload,
                delivery_tag,
                ack_status,
                consumed_at,
                consumed_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                queue_name,
                event_data.get("event_type", "unknown"),
                json.dumps(event_data.get("payload", {})),
                delivery_tag,
                "acknowledged",
                datetime.now(timezone.utc).isoformat(),
                consumer_role,
            ),
        )
        conn.commit()

    if queue_name == "allocare.filesystem.events":
        store_file_system_event(event_data)
    elif queue_name.startswith("allocare.group."):
        store_group_message(queue_name, event_data, consumer_role)
    elif queue_name == SYSTEM_NOTIFICATION_QUEUE:
        store_system_notification(event_data, consumer_role)


def store_group_message(queue_name: str, event_data: dict, consumer_role: str) -> None:
    """Store group communication messages for Chapter 4 group communication."""
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO group_messages (
                group_name,
                routing_key,
                title,
                message,
                payload,
                created_by,
                created_at,
                consumed_at,
                consumed_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_data.get("group_name", queue_name.removeprefix("allocare.group.")),
                queue_name,
                event_data.get("title", "Group announcement"),
                event_data.get("message", ""),
                json.dumps(event_data.get("payload", {})),
                event_data.get("created_by", event_data.get("payload", {}).get("issuer", "system")),
                event_data.get("timestamp", datetime.now(timezone.utc).isoformat()),
                datetime.now(timezone.utc).isoformat(),
                consumer_role,
            ),
        )
        conn.commit()


def store_system_notification(event_data: dict, consumer_role: str) -> None:
    """Store OS-level lifecycle notifications for Chapter 7 event notification mechanisms."""
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO system_notifications (
                notification_type,
                source,
                payload,
                consumed_at,
                consumed_by
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                event_data.get("event_type", "system.notification"),
                event_data.get("source", "unknown"),
                json.dumps(event_data),
                datetime.now(timezone.utc).isoformat(),
                consumer_role,
            ),
        )
        conn.commit()


def store_file_system_event(event_data: dict) -> None:
    """Store file system event in dedicated table (Chapter 7: OS Support)."""
    try:
        event_details = event_data.get("data", {})
        with get_db_connection() as conn:
            conn.execute(
                """
                INSERT INTO file_system_events (
                    event_type,
                    file_path,
                    file_name,
                    file_size,
                    event_category,
                    file_extension,
                    processed_at,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_data.get("event_type", "unknown"),
                    event_details.get("file_path", ""),
                    event_details.get("file_name", ""),
                    event_details.get("file_size", 0),
                    event_details.get("event_category", ""),
                    event_details.get("file_extension", ""),
                    datetime.now(timezone.utc).isoformat(),
                    event_data.get("timestamp", datetime.now(timezone.utc).isoformat()),
                ),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"[!] Error storing file system event: {exc}")


def generate_alerts(queue_name: str, event_data: dict) -> None:
    """Generate alerts for critical events."""
    event_type = event_data.get("event_type", "")
    payload = event_data.get("payload", {})

    with get_db_connection() as conn:
        if event_type == "patient.admission" and payload.get("severity") == "critical":
            conn.execute(
                """
                INSERT INTO alerts (
                    alert_type,
                    severity,
                    title,
                    description,
                    source_data,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "critical_patient",
                    "critical",
                    f"Critical Patient: {payload.get('patient_id')}",
                    f"Patient {payload.get('patient_id')} admitted with critical severity in {payload.get('facility_unit')}",
                    json.dumps(payload),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

        if event_type == "bed.capacity":
            max_capacity = payload.get("max_capacity", 0)
            conn.execute(
                """
                INSERT INTO alerts (
                    alert_type,
                    severity,
                    title,
                    description,
                    source_data,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "capacity_update",
                    "info",
                    f"Capacity Updated: {payload.get('facility_unit')}",
                    f"Facility unit {payload.get('facility_unit')} capacity set to {max_capacity} beds",
                    json.dumps(payload),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

        if event_type == "doctor.registration" and payload.get("availability") == "off-duty":
            conn.execute(
                """
                INSERT INTO alerts (
                    alert_type,
                    severity,
                    title,
                    description,
                    source_data,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "doctor_unavailable",
                    "warning",
                    f"Doctor Off Duty: {payload.get('doctor_id')}",
                    f"Dr. {payload.get('doctor_name', 'Unknown')} ({payload.get('specialty')}) is now off duty",
                    json.dumps(payload),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

        if queue_name == "allocare.filesystem.events":
            event_cat = event_data.get("data", {}).get("event_category", "data")
            file_name = event_data.get("data", {}).get("file_name", "unknown")
            severity = {
                "file_created": "info",
                "file_modified": "info",
                "file_deleted": "warning",
            }.get(event_type, "info")

            conn.execute(
                """
                INSERT INTO alerts (
                    alert_type,
                    severity,
                    title,
                    description,
                    source_data,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    f"filesystem_{event_type}",
                    severity,
                    f"File System Event: {event_type.replace('file_', '').title()}",
                    f"{event_cat.title()} file '{file_name}' was {event_type.replace('file_', '')}",
                    json.dumps(event_data.get("data", {})),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

        conn.commit()


def update_dashboard_metrics(queue_name: str, event_data: dict) -> None:
    """Maintain live metrics for the real-time dashboard role."""
    event_type = event_data.get("event_type", "unknown")
    now = datetime.now(timezone.utc).isoformat()

    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO dashboard_event_metrics (metric_key, metric_value, updated_at)
            VALUES ('total_events', 1, ?)
            ON CONFLICT(metric_key) DO UPDATE SET
                metric_value = metric_value + 1,
                updated_at = excluded.updated_at
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO dashboard_event_metrics (metric_key, metric_value, updated_at)
            VALUES (?, 1, ?)
            ON CONFLICT(metric_key) DO UPDATE SET
                metric_value = metric_value + 1,
                updated_at = excluded.updated_at
            """,
            (event_type, now),
        )
        conn.commit()


def handle_audit_logger(queue_name: str, event_data: dict, delivery_tag: str) -> None:
    store_consumed_event(queue_name, event_data, "audit_logger", delivery_tag)


def handle_alert_system(queue_name: str, event_data: dict, delivery_tag: str) -> None:
    generate_alerts(queue_name, event_data)


def handle_real_time_dashboard(queue_name: str, event_data: dict, delivery_tag: str) -> None:
    update_dashboard_metrics(queue_name, event_data)


ROLE_HANDLERS = {
    "audit_logger": handle_audit_logger,
    "alert_system": handle_alert_system,
    "real_time_dashboard": handle_real_time_dashboard,
}


def create_callback(role_name: str):
    handler = ROLE_HANDLERS[role_name]

    def callback(ch, method, properties, body) -> None:
        try:
            message = json.loads(body)
            queue_name = method.routing_key

            print(f"[*] {role_name} received from {queue_name}: {message.get('event_type')}")
            handler(queue_name, message, str(method.delivery_tag))
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[SUCCESS] {role_name} acknowledged delivery_tag: {method.delivery_tag}")
        except Exception as exc:  # noqa: BLE001
            print(f"[!] Error processing message in {role_name}: {exc}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    return callback


def connect_consumer(role_name: str) -> None:
    """Connect to RabbitMQ and start the selected role-specific worker."""
    if role_name not in ROLES:
        raise ValueError(f"Unknown consumer role: {role_name}")

    role_config = ROLES[role_name]
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    while True:
        connection = None
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type="topic", durable=True)
            channel.basic_qos(prefetch_count=1)

            worker_queue = role_config["queue"]
            channel.queue_declare(queue=worker_queue, durable=True)

            for routing_key in role_config["bindings"]:
                channel.queue_bind(exchange=RABBITMQ_EXCHANGE, queue=worker_queue, routing_key=routing_key)

            channel.basic_consume(
                queue=worker_queue,
                on_message_callback=create_callback(role_name),
                auto_ack=False,
            )

            print("=" * 60)
            print(f"Allocare {role_config['description']} Service")
            print(f"Queue: {worker_queue}")
            print(f"Bindings: {', '.join(role_config['bindings'])}")
            print("Waiting for messages... (Press CTRL+C to exit)")
            print("=" * 60)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as exc:
            print(f"[!] Connection error in {role_name}: {exc}")
            print("[*] Retrying in 5 seconds...")
            time.sleep(5)
        except KeyboardInterrupt:
            print(f"\n[*] {role_name} stopped")
            if connection and not connection.is_closed:
                connection.close()
            break
        finally:
            if connection and not connection.is_closed:
                connection.close()


if __name__ == "__main__":
    init_db()
    connect_consumer(CONSUMER_ROLE)
