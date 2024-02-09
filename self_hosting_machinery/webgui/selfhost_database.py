import os
import uuid
import logging
import asyncio

from datetime import datetime
from typing import Dict, Any, Iterable, List, Union, AsyncIterator

from more_itertools import chunked
from scyllapy import Scylla, InlineBatch, ExecutionProfile, Consistency, SerialConsistency
from scyllapy.query_builder import Insert, Select


os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'


class ScyllaModel:
    INSERT_BSIZE: int = 128

    def __init__(self, *args, **kwargs):
        self.is_ready = False

    async def init(self, scylla: Scylla) -> None:
        await scylla.execute(self.create_table_query())
        self.is_ready = True

    async def insert(self, scylla: Scylla, data: Iterable[Dict]) -> None:
        for data_b in chunked(data, self.INSERT_BSIZE):
            batch = InlineBatch()
            for row in data_b:
                i = Insert(self.name)
                [i.set(k, v) for k, v in row.items()]
                i.add_to_batch(batch)
            await scylla.batch(batch)

    @property
    def name(self) -> str:
        raise NotImplementedError()

    def create_table_query(self) -> str:
        raise NotImplementedError()


class ScyllaBatchInserter:
    def __init__(
            self,
            scylla_service: Any,
            b_size: int = 128
    ):
        self._scylla_service = scylla_service
        self._b_size = b_size
        self._cache = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for to, data in self._cache.items():
            await self._scylla_service.insert(data=data, to=to)

    async def insert(self, data: Union[Dict, Iterable[Dict]], to: str) -> None:
        data = [data] if not isinstance(data, list) else data
        for d in data:
            self._cache.setdefault(to, []).append(d)
            if len(self._cache[to]) >= self._b_size:
                await self._insert_records(self._cache[to], to)

    async def _insert_records(self, data: List[Dict], to: str):
        await self._scylla_service.insert(data=data, to=to)
        self._cache[to] = []


class UsersAccessControl(ScyllaModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        return "users_access_control"

    def create_table_query(self) -> str:
        return """
CREATE TABLE IF NOT EXISTS users_access_control (
    account text PRIMARY KEY,
    team text,
    api_key text
);
        """


class TelemetryNetwork(ScyllaModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        return "telemetry_network"

    def create_table_query(self) -> str:
        return """
CREATE TABLE IF NOT EXISTS telemetry_network (
    id text PRIMARY KEY,
    tenant_name text,
    team text,
    ts_reported timestamp,
    ip text,
    enduser_client_version text,
    counter int,
    error_message text,
    scope text,
    success boolean,
    url text,
    teletype text,
    ts_start int,
    ts_end int
);
        """


class TelemetrySnippets(ScyllaModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        return "telemetry_snippets"

    def create_table_query(self) -> str:
        return """
CREATE TABLE IF NOT EXISTS telemetry_snippets (
    id text PRIMARY KEY,
    tenant_name text,
    team text,
    ts_reported timestamp,
    ip text,
    enduser_client_version text,
    model text,
    corrected_by_user text,
    remaining_percentage float,
    created_ts int,
    accepted_ts int,
    finished_ts int,
    grey_text text,
    cursor_character int,
    cursor_file text,
    cursor_line int,
    multiline boolean,
    sources text,
    teletype text
);
        """


class TelemetryRobotHuman(ScyllaModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        return "telemetry_robot_human"

    def create_table_query(self) -> str:
        return """
CREATE TABLE IF NOT EXISTS telemetry_robot_human (
    id text PRIMARY KEY,
    tenant_name text,
    team text,
    ts_reported timestamp,
    ip text,
    enduser_client_version text,
    completions_cnt int,
    file_extension text,
    human_characters int,
    model text,
    robot_characters int,
    teletype text,
    ts_start int,
    ts_end int
);
        """


class TelemetryCompCounters(ScyllaModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        return "telemetry_comp_counters"

    def create_table_query(self) -> str:
        return """
CREATE TABLE IF NOT EXISTS telemetry_comp_counters (
    id text PRIMARY KEY,
    tenant_name text,
    team text,
    ts_reported timestamp,
    ip text,
    enduser_client_version text,
    counters_json_text text,
    file_extension text,
    model text,
    multiline boolean,
    teletype text,
    ts_end int,
    ts_start int
);
        """


class DisableLogger:

    def __enter__(self):
        logging.disable(logging.CRITICAL)

    def __exit__(self, exit_type, exit_value, exit_traceback):
        logging.disable(logging.NOTSET)


class RefactDatabase:
    KEYSPACE = os.environ.get("REFACT_KEYSPACE", "smc")
    HOST = os.environ.get("REFACT_DATABASE_HOST", "127.0.0.1")
    PORT = int(os.environ.get("REFACT_DATABASE_PORT", 9042))

    def __init__(self):
        self._scylla = None
        self._query_profile = ExecutionProfile(
            consistency=Consistency.LOCAL_ONE,
            serial_consistency=SerialConsistency.LOCAL_SERIAL,
            request_timeout=5
        )

    async def connect(self):
        # NOTE: this is a hack to wait for a db to be ready
        while True:
            try:
                self._scylla = Scylla(
                    contact_points=[f"{self.HOST}:{self.PORT}"],
                    username="cassandra",
                    password="cassandra",
                    default_execution_profile=self._query_profile,
                )
                await self._scylla.startup()
                break
            except Exception as e:
                logging.warning(f"No database available on {self.HOST}:{self.PORT}; error: {e} "
                                f"sleep for 10 seconds...")
                await asyncio.sleep(10)

        await self._create_keyspace_if_not_exists(self.KEYSPACE)
        await self.scylla.use_keyspace(self.KEYSPACE)

    def __del__(self):
        if self._scylla:
            asyncio.shield(self._scylla.shutdown())

    async def _create_keyspace_if_not_exists(self, keyspace: str) -> None:
        await self.scylla.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '2' }}
        """)

    @property
    def scylla(self) -> Scylla:
        return self._scylla


class StatisticsService:

    def __init__(self, database: RefactDatabase):
        self._database: RefactDatabase = database
        self._net: ScyllaModel = TelemetryNetwork()
        self._snip: ScyllaModel = TelemetrySnippets()
        self._rh: ScyllaModel = TelemetryRobotHuman()
        self._comp: ScyllaModel = TelemetryCompCounters()

    async def init_models(self):
        await self._net.init(self.scylla)
        await self._snip.init(self.scylla)
        await self._rh.init(self.scylla)
        await self._comp.init(self.scylla)

    @property
    def is_ready(self) -> bool:
        return all([
            self._net.is_ready,
            self._snip.is_ready,
            self._rh.is_ready,
            self._comp.is_ready,
        ])

    async def insert(self, data: Iterable[Dict], to: str) -> None:
        data: Iterable[Dict[str, Any]] = (
            {
                "id": str(uuid.uuid1()),
                "ts_reported": datetime.now(),
                **d,
            } for d in data
        )
        if to == "net":
            await self._net.insert(self.scylla, data)
        elif to == "snip":
            await self._snip.insert(self.scylla, data)
        elif to == "rh":
            await self._rh.insert(self.scylla, data)
        elif to == "comp":
            await self._comp.insert(self.scylla, data)
        else:
            raise NotImplementedError(f"cannot insert to {to}; type {to} does not exist")

    async def get_robot_human_for_account(self, account: str) -> AsyncIterator[Dict]:
        rows = await Select("telemetry_robot_human")\
            .where("tenant_name =?", [account])\
            .allow_filtering()\
            .execute(self.scylla, paged=True)
        async for r in rows:
            yield {
                "id": 0,
                "tenant_name": r["tenant_name"],
                "ts_reported": int(r["ts_reported"].timestamp()),
                "ip": r["ip"],
                "enduser_client_version": r["enduser_client_version"],
                "completions_cnt": r["completions_cnt"],
                "file_extension": r["file_extension"],
                "human_characters": r["human_characters"],
                "model": r["model"],
                "robot_characters": r["robot_characters"],
                "teletype": r["teletype"],
                "ts_start": r["ts_start"],
                "ts_end": r["ts_end"],
            }

    @property
    def scylla(self) -> Scylla:
        return self._database.scylla
