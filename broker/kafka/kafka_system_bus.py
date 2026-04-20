"""Kafka SystemBus."""
import json
import threading
import time
import asyncio
import os
from typing import Callable, Dict, Any, Optional
from uuid import uuid4
from concurrent.futures import Future

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError, NoBrokersAvailable
    KAFKA_AVAILABLE = True
except ImportError:
    KafkaProducer = None
    KafkaConsumer = None

    class KafkaError(Exception):
        pass

    class NoBrokersAvailable(Exception):
        pass

    KAFKA_AVAILABLE = False

from broker.system_bus import SystemBus
from broker.config import get_kafka_bootstrap


class KafkaSystemBus(SystemBus):

    def __init__(
        self, 
        bootstrap_servers: str = None, 
        client_id: str = "system_bus",
        group_id: str = None,
        username: str = None,
        password: str = None,
        security_protocol: str = None,
        sasl_mechanism: str = None
    ):
        if not KAFKA_AVAILABLE:
            raise ImportError(
                "kafka-python is not installed. Install it with: pip install kafka-python"
            )

        self.bootstrap_servers = bootstrap_servers or get_kafka_bootstrap()
        self.client_id = client_id
        self.group_id = group_id or f"{client_id}_group"
        self.username = (
            username
            or os.environ.get("KAFKA_SASL_USERNAME")
            or os.environ.get("BROKER_USER")
        )
        self.password = (
            password
            or os.environ.get("KAFKA_SASL_PASSWORD")
            or os.environ.get("BROKER_PASSWORD")
        )
        self.security_protocol = (
            security_protocol
            or os.environ.get("KAFKA_SECURITY_PROTOCOL")
            or ("SASL_PLAINTEXT" if self.username and self.password else "PLAINTEXT")
        )
        self.sasl_mechanism = (
            sasl_mechanism
            or os.environ.get("KAFKA_SASL_MECHANISM")
            or "PLAIN"
        )
        self._producer: Optional[KafkaProducer] = None
        self._consumers: Dict[str, KafkaConsumer] = {}
        self._callbacks: Dict[str, Callable[[Dict[str, Any]], None]] = {}
        self._consumer_threads: Dict[str, threading.Thread] = {}
        self._running: Dict[str, bool] = {}
        self._pending_requests: Dict[str, Future] = {}
        self._pending_lock = threading.Lock()
        self._reply_topic = f"replies.{client_id}.{uuid4().hex[:8]}"
        self._started = False

    def get_sasl_config(self) -> dict:
        """SASL-конфиг для producer/consumer, если заданы username/password."""
        if self.username and self.password:
            return {
                'security_protocol': self.security_protocol,
                'sasl_mechanism': self.sasl_mechanism,
                'sasl_plain_username': self.username,
                'sasl_plain_password': self.password
            }
        if self.security_protocol and self.security_protocol != "PLAINTEXT":
            return {'security_protocol': self.security_protocol}
        return {}

    def _init_producer(self):
        """Создаёт Kafka producer при первой отправке."""
        if self._producer is None:
            config = {
                'bootstrap_servers': self.bootstrap_servers,
                'client_id': self.client_id,
                'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                'acks': 'all',
                **self.get_sasl_config()
            }
            self._producer = KafkaProducer(**config)

    def _init_producer_with_retry(self):
        """Wait for Kafka to become available during cold starts."""
        max_attempts = max(1, int(os.environ.get("KAFKA_STARTUP_MAX_ATTEMPTS", "20")))
        retry_delay = max(
            0.1,
            float(os.environ.get("KAFKA_STARTUP_RETRY_DELAY_SEC", "3")),
        )
        last_error: Optional[Exception] = None

        for attempt in range(1, max_attempts + 1):
            try:
                self._init_producer()
                return
            except Exception as exc:
                last_error = exc
                if attempt == max_attempts:
                    break
                print(
                    f"Kafka broker not ready for client_id={self.client_id} "
                    f"(attempt {attempt}/{max_attempts}): {exc}. "
                    f"Retrying in {retry_delay:.1f}s..."
                )
                time.sleep(retry_delay)

        if last_error is not None:
            raise last_error

    def start(self) -> None:
        """Запускает bus, создаёт reply-топик и подписывается на ответы."""
        if self._started:
            return
        self._init_producer_with_retry()
        try:
            self._producer.send(self._reply_topic, {"_init": True}).get(timeout=10)
        except Exception:
            pass
        self._producer.flush()
        time.sleep(1.0)
        self.subscribe(self._reply_topic, self._handle_reply)
        
        self._started = True
        print(f"KafkaSystemBus started. Reply topic: {self._reply_topic}")

    def stop(self) -> None:
        """Останавливает consumers и producer."""
        for topic in list(self._running.keys()):
            self._running[topic] = False
        for topic, thread in list(self._consumer_threads.items()):
            thread.join(timeout=2)
        for consumer in self._consumers.values():
            try:
                consumer.close()
            except Exception:
                pass
        if self._producer:
            try:
                self._producer.close()
            except Exception:
                pass
        
        self._consumers.clear()
        self._callbacks.clear()
        self._consumer_threads.clear()
        self._running.clear()
        self._started = False
        
        print("KafkaSystemBus stopped")

    def publish(self, topic: str, message: Dict[str, Any]) -> bool:
        """Публикует сообщение в топик."""
        self._init_producer()
        try:
            future = self._producer.send(topic, message)
            future.get(timeout=10)
            return True
        except KafkaError as e:
            print(f"Error publishing to Kafka topic {topic}: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error publishing to {topic}: {e}")
            return False

    def _consumer_loop(self, topic: str):
        """Цикл чтения сообщений из топика и вызова callback."""
        consumer = self._consumers.get(topic)
        callback = self._callbacks.get(topic)

        if not consumer or not callback:
            return

        while self._running.get(topic, False):
            try:
                messages = consumer.poll(timeout_ms=1000)
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            message = record.value
                            callback(message)
                        except Exception as e:
                            print(f"Error processing message from {topic}: {e}")
            except Exception as e:
                if self._running.get(topic, False):
                    print(f"Error in consumer loop for {topic}: {e}")
                    time.sleep(1)

    def subscribe(self, topic: str, callback: Callable[[Dict[str, Any]], None]) -> bool:
        """Подписывается на топик, callback вызывается при получении сообщения."""
        if topic in self._callbacks:
            print(f"Already subscribed to {topic}")
            return True
        
        self._callbacks[topic] = callback
        try:
            is_reply_topic = topic.startswith("replies.")
            group_suffix = str(uuid4())[:8] if is_reply_topic else "v1"
            
            config = {
                'bootstrap_servers': self.bootstrap_servers,
                'client_id': f"{self.client_id}_{topic.replace('.', '_')}",
                'group_id': f"{self.group_id}_{topic.replace('.', '_')}_{group_suffix}",
                'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
                'auto_offset_reset': 'earliest',
                'enable_auto_commit': True,
                **self.get_sasl_config()
            }
            consumer = KafkaConsumer(topic, **config)

            # Ждём фактического назначения партиций (group join/rebalance),
            # иначе сообщения, опубликованные сразу после subscribe(), теряются.
            assign_timeout = float(
                os.environ.get("KAFKA_SUBSCRIBE_ASSIGN_TIMEOUT_SEC", "10")
            )
            deadline = time.time() + assign_timeout
            while not consumer.assignment() and time.time() < deadline:
                consumer.poll(timeout_ms=200)
            if not consumer.assignment():
                print(
                    f"Warning: consumer for {topic} did not receive partition "
                    f"assignment within {assign_timeout}s"
                )

            self._consumers[topic] = consumer
            self._running[topic] = True
            thread = threading.Thread(
                target=self._consumer_loop,
                args=(topic,),
                daemon=True,
                name=f"kafka-consumer-{topic}"
            )
            thread.start()
            self._consumer_threads[topic] = thread
            return True
        except Exception as e:
            print(f"Error subscribing to {topic}: {e}")
            return False

    def unsubscribe(self, topic: str) -> bool:
        """Отписывается от топика."""
        self._running[topic] = False
        
        if topic in self._consumer_threads:
            thread = self._consumer_threads[topic]
            thread.join(timeout=2)
            del self._consumer_threads[topic]
        
        if topic in self._callbacks:
            del self._callbacks[topic]
        
        if topic in self._consumers:
            try:
                self._consumers[topic].close()
            except Exception:
                pass
            del self._consumers[topic]
        
        return True

    def _handle_reply(self, message: Dict[str, Any]):
        """Обрабатывает входящий ответ по correlation_id, завершает pending Future."""
        correlation_id = message.get("correlation_id")
        if not correlation_id:
            return
        
        with self._pending_lock:
            if correlation_id in self._pending_requests:
                future = self._pending_requests.pop(correlation_id)
                future.set_result(message)

    def request(
        self, 
        topic: str, 
        message: Dict[str, Any], 
        timeout: float = 30.0
    ) -> Optional[Dict[str, Any]]:
        """Синхронный request/response: отправляет запрос, ждёт ответ до timeout."""
        if not self._started:
            self.start()
        correlation_id = str(uuid4())
        future: Future = Future()
        with self._pending_lock:
            self._pending_requests[correlation_id] = future
        request_message = {
            **message,
            "correlation_id": correlation_id,
            "reply_to": self._reply_topic
        }
        if not self.publish(topic, request_message):
            with self._pending_lock:
                self._pending_requests.pop(correlation_id, None)
            return None
        try:
            result = future.result(timeout=timeout)
            return result
        except TimeoutError:
            with self._pending_lock:
                self._pending_requests.pop(correlation_id, None)
            print(f"Request to {topic} timed out after {timeout}s")
            return None
        except Exception as e:
            with self._pending_lock:
                self._pending_requests.pop(correlation_id, None)
            print(f"Error waiting for response: {e}")
            return None

    def request_async(
        self, 
        topic: str, 
        message: Dict[str, Any], 
        timeout: float = 30.0
    ) -> asyncio.Future:
        """Асинхронная обёртка над request()."""
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(
            None, 
            lambda: self.request(topic, message, timeout)
        )
