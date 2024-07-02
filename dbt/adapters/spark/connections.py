import codecs
from contextlib import contextmanager
import json
import pickle
from textwrap import dedent

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    ConnectionState,
    Connection,
    Credentials,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtConfigError, DbtRuntimeError, DbtDatabaseError

from dbt_common.utils.encoding import DECIMALS
from dbt.adapters.spark import __version__
import websockets.sync.client

try:
    from TCLIService.ttypes import TOperationState as ThriftState
    from thrift.transport import THttpClient
    from pyhive import hive
except ImportError:
    ThriftState = None
    THttpClient = None
    hive = None
try:
    import pyodbc
except ImportError:
    pyodbc = None
from datetime import datetime
import sqlparams
from dbt_common.dataclass_schema import StrEnum
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union, Tuple, List, Generator, Iterable, Sequence

from abc import ABC, abstractmethod

try:
    from thrift.transport.TSSLSocket import TSSLSocket
    import thrift
    import ssl
    import thrift_sasl
    from puresasl.client import SASLClient
except ImportError:
    pass  # done deliberately: setting modules to None explicitly violates MyPy contracts by degrading type semantics
import time
from uuid import uuid4


logger = AdapterLogger("Spark")

NUMBERS = DECIMALS + (int, float)


def _build_odbc_connnection_string(**kwargs: Any) -> str:
    return ";".join([f"{k}={v}" for k, v in kwargs.items()])


class SparkConnectionMethod(StrEnum):
    THRIFT = "thrift"
    HTTP = "http"
    ODBC = "odbc"
    SESSION = "session"


@dataclass
class SparkCredentials(Credentials):
    host: Optional[str] = None
    schema: Optional[str] = None
    method: SparkConnectionMethod = None  # type: ignore
    database: Optional[str] = None
    driver: Optional[str] = None
    cluster: Optional[str] = None
    endpoint: Optional[str] = None
    token: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    port: int = 443
    auth: Optional[str] = None
    kerberos_service_name: Optional[str] = None
    organization: str = "0"
    connect_retries: int = 0
    connect_timeout: int = 10
    use_ssl: bool = False
    server_side_parameters: Dict[str, str] = field(default_factory=dict)
    retry_all: bool = False

    @classmethod
    def __pre_deserialize__(cls, data: Any) -> Any:
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data

    @property
    def cluster_id(self) -> Optional[str]:
        return self.cluster

    def __post_init__(self) -> None:
        if self.method is None:
            raise DbtRuntimeError("Must specify `method` in profile")
        if self.host is None:
            raise DbtRuntimeError("Must specify `host` in profile")
        if self.schema is None:
            raise DbtRuntimeError("Must specify `schema` in profile")

        # spark classifies database and schema as the same thing
        if self.database is not None and self.database != self.schema:
            raise DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Spark, database must be omitted or have the same value as"
                f" schema."
            )
        self.database = None

        if self.method == SparkConnectionMethod.ODBC:
            try:
                import pyodbc  # noqa: F401
            except ImportError as e:
                raise DbtRuntimeError(
                    f"{self.method} connection method requires "
                    "additional dependencies. \n"
                    "Install the additional required dependencies with "
                    "`pip install dbt-spark[ODBC]`\n\n"
                    f"ImportError({e.msg})"
                ) from e

        if self.method == SparkConnectionMethod.ODBC and self.cluster and self.endpoint:
            raise DbtRuntimeError(
                "`cluster` and `endpoint` cannot both be set when"
                f" using {self.method} method to connect to Spark"
            )

        if (
            self.method == SparkConnectionMethod.HTTP
            or self.method == SparkConnectionMethod.THRIFT
        ) and not (ThriftState and THttpClient and hive):
            raise DbtRuntimeError(
                f"{self.method} connection method requires "
                "additional dependencies. \n"
                "Install the additional required dependencies with "
                "`pip install dbt-spark[PyHive]`"
            )

        if self.method == SparkConnectionMethod.SESSION:
            try:
                import pyspark  # noqa: F401
            except ImportError as e:
                raise DbtRuntimeError(
                    f"{self.method} connection method requires "
                    "additional dependencies. \n"
                    "Install the additional required dependencies with "
                    "`pip install dbt-spark[session]`\n\n"
                    f"ImportError({e.msg})"
                ) from e

        if self.method != SparkConnectionMethod.SESSION:
            self.host = self.host.rstrip("/")

        self.server_side_parameters = {
            str(key): str(value) for key, value in self.server_side_parameters.items()
        }

    @property
    def type(self) -> str:
        return "spark"

    @property
    def unique_field(self) -> str:
        return self.host  # type: ignore

    def _connection_keys(self) -> Tuple[str, ...]:
        return "host", "port", "cluster", "endpoint", "schema", "organization"


class SparkConnectionWrapper(ABC):
    @abstractmethod
    def cursor(self) -> "SparkConnectionWrapper":
        pass

    @abstractmethod
    def cancel(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def rollback(self) -> None:
        pass

    @abstractmethod
    def fetchall(self) -> Optional[List]:
        pass

    @abstractmethod
    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        pass

    @property
    @abstractmethod
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        pass


class PyhiveConnectionWrapper(SparkConnectionWrapper):
    """Wrap a Spark connection in a way that no-ops transactions"""

    # https://forums.databricks.com/questions/2157/in-apache-spark-sql-can-we-roll-back-the-transacti.html  # noqa

    handle: websockets.client.ClientConnection

    def __init__(self, handle: websockets.client.ClientConnection) -> None:
        self.handle = handle

    def cursor(self) -> "PyhiveConnectionWrapper":
        logger.debug("NotImplemented: cursor")
        # self._cursor = self.handle.cursor()
        return self

    def cancel(self) -> None:
        logger.debug("NotImplemented: cancel")
        # TODO MSS: SEND INTERRUPT
        # if self._cursor:
        #     # Handle bad response in the pyhive lib when
        #     # the connection is cancelled
        #     try:
        #         self._cursor.cancel()
        #     except EnvironmentError as exc:
        #         logger.debug("Exception while cancelling query: {}".format(exc))

    def close(self) -> None:
        # if self._cursor:
        #     # Handle bad response in the pyhive lib when
        #     # the connection is cancelled
        #     try:
        #         self._cursor.close()
        #     except EnvironmentError as exc:
        #         logger.debug("Exception while closing cursor: {}".format(exc))
        self.handle.close_socket()

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    def fetchall(self) -> List["pyodbc.Row"]:
        logger.debug("fetchall")
        print('description')
        fetchall = []
        obj = self._exec('fetchall=cursor.fetchall()', outputs=['fetchall'])
        if obj is not None and 'objs' in obj and 'fetchall' in obj['objs']:
            fetchall = pickle.loads(codecs.decode(obj['objs']['fetchall'].encode(), "base64"))
        
        logger.debug(fetchall)
        return fetchall
        #assert self._cursor, "Cursor not available"
        #return self._cursor.fetchall()

    def _exec(self, cmd: str, inputs: Dict = {}, outputs: List[Any] = []):
        
        final_inputs = {}
        for key in inputs:
            print(key)
            final_inputs[key] = codecs.encode(pickle.dumps(inputs[key]), "base64").decode()

        req = {
            "type": "code",
            "exec_cmd": cmd,
            "inputs": final_inputs,
            "outputs": outputs
        }
        
        self.handle.send(json.dumps(req))

        for message in self.handle:
            if message is None:
                raise DbtDatabaseError("Disconnected")

            msg = json.loads(message)
            state = msg['state']

            if 'COMPLETED' == state:
                logger.debug("query complete")
                logger.debug(msg)
                return msg
            elif 'BAD_INPUT' == state:
                logger.debug(msg)
                if 'stdout' in msg:
                    raise DbtDatabaseError("Execution failed: {}".format(msg['stdout']))
            elif 'CODE_ERROR' == state or 'ERROR' == state:
                logger.debug(msg)
                if 'stdout' in msg:
                    raise DbtDatabaseError("Query failed with status: {}".format(msg['stdout']))
                raise DbtDatabaseError("Query failed with no output")

    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        logger.debug(sql)

        code = f'''
        from TCLIService.ttypes import TOperationState as ThriftState
        STATE_PENDING = [
            ThriftState.INITIALIZED_STATE,
            ThriftState.RUNNING_STATE,
            ThriftState.PENDING_STATE,
        ]

        STATE_SUCCESS = [
            ThriftState.FINISHED_STATE,
        ]

        cursor.execute(sql, bindings=bindings, async_=True)
        poll_state = cursor.poll()
        state = poll_state.operationState

        while state in STATE_PENDING:
            print("Poll status: {{}}, sleeping".format(state))

            poll_state = cursor.poll()
            state = poll_state.operationState

        if poll_state.errorMessage:
            print("Poll response: {{}}".format(poll_state))
            print("Poll status: {{}}".format(state))
            raise Exception(poll_state.errorMessage)

        elif state not in STATE_SUCCESS:
            status_type = ThriftState._VALUES_TO_NAMES.get(state, "Unknown<{{!r}}>".format(state))
            raise Exception("Query failed with status: {{}}".format(status_type))
        '''

        sql_cmd=f'sql="""{sql}"""'
        inputs = {}

        if bindings is not None:
            inputs['bindings'] = [self._fix_binding(binding) for binding in bindings]
        else:
            inputs['bindings'] = []

        self._exec(sql_cmd + dedent(code), inputs=inputs)


    @classmethod
    def _fix_binding(cls, value: Any) -> Union[float, str]:
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:
            return value

    @property
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        print('description')
        desc = []
        obj = self._exec('desc=cursor.description', outputs=['desc'])
        if obj is not None and 'objs' in obj and 'desc' in obj['objs']:
            desc = pickle.loads(codecs.decode(obj['objs']['desc'].encode(), "base64"))
        
        logger.debug(desc)
        return desc
        # assert self._cursor, "Cursor not available"
        # return self._cursor.description


class SparkConnectionManager(SQLConnectionManager):
    TYPE = "spark"

    API_URL = 'wss://cpd-cpd-instance.apps.mss-exp2.cp.fyre.ibm.com/lakehouse/api/v2/spark_engines/spark837/clusters/b4663f71-9705-4047-bba8-27db4afa4981/connect'
    API_HEADERS = {"lhInstanceId":"1718953267559731", "Authorization": "ZenApiKey Y3BhZG1pbjpxeVZJWVlEMDdPeUt6Z2xUTU9ESzBBT1lRenRrRFhKNEJTeW5uT0JnCg=="}

    @contextmanager
    def exception_handler(self, sql: str) -> Generator[None, None, None]:
        try:
            yield

        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, "status"):
                msg = thrift_resp.status.errorMessage
                raise DbtRuntimeError(msg)
            else:
                raise DbtRuntimeError(str(exc))

    def cancel(self, connection: Connection) -> None:
        print("Cancelling")
        connection.handle.close()

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        # https://github.com/dbt-labs/dbt-spark/issues/142
        message = "OK"
        return AdapterResponse(_message=message)

    # No transactions on Spark....
    def add_begin_query(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: add_begin_query")

    def add_commit_query(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: add_commit_query")

    def commit(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: commit")

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    @classmethod
    def validate_creds(cls, creds: Any, required: Iterable[str]) -> None:
        method = creds.method

        for key in required:
            if not hasattr(creds, key):
                raise DbtConfigError(
                    "The config '{}' is required when using the {} method"
                    " to connect to Spark".format(key, method)
                )

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        print('open')
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        #cls.validate_creds(connection.credentials, ["poc"])

        wsclient = None
        print("Initializing")
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            wsclient = websockets.sync.client.connect(
                cls.API_URL, 
                ssl_context=ssl_context,
                open_timeout=120,
                close_timeout=120,
                additional_headers=cls.API_HEADERS)
            print("Got connection")
            connection.handle = PyhiveConnectionWrapper(wsclient)
            
            print("Initializing Pyhive")
            code = dedent("""
            from pyhive import hive
            cursor = hive.connect('localhost').cursor()
            """)
            
            status = connection.handle._exec(code)
            if status is None:
                raise ConnectionError("Failed to connect to Thrift Server")

        except Exception as e:
            if connection.handle:
                connection.handle.close()
            raise ConnectionError(e)

        
        connection.state = ConnectionState.OPEN
        print('open complete')
        return connection

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[type, str]) -> str:
        """
        :param Union[type, str] type_code: The sql to execute.
            * type_code is a python type (!) in pyodbc https://github.com/mkleehammer/pyodbc/wiki/Cursor#description, and a string for other spark runtimes.
            * ignoring the type annotation on the signature for this adapter instead of updating the base class because this feels like a really special case.
        :return: stringified the cursor type_code
        :rtype: str
        """
        if isinstance(type_code, str):
            return type_code
        return type_code.__name__.upper()


def build_ssl_transport(
    host: str,
    port: int,
    username: str,
    auth: str,
    kerberos_service_name: str,
    password: Optional[str] = None,
) -> "thrift_sasl.TSaslClientTransport":
    transport = None
    if port is None:
        port = 10000
    if auth is None:
        auth = "NONE"
    socket = TSSLSocket(host, port, cert_reqs=ssl.CERT_NONE)
    if auth == "NOSASL":
        # NOSASL corresponds to hive.server2.authentication=NOSASL
        # in hive-site.xml
        transport = thrift.transport.TTransport.TBufferedTransport(socket)
    elif auth in ("LDAP", "KERBEROS", "NONE", "CUSTOM"):
        # Defer import so package dependency is optional
        if auth == "KERBEROS":
            # KERBEROS mode in hive.server2.authentication is GSSAPI
            # in sasl library
            sasl_auth = "GSSAPI"
        else:
            sasl_auth = "PLAIN"
            if password is None:
                # Password doesn't matter in NONE mode, just needs
                # to be nonempty.
                password = "x"

        def sasl_factory() -> SASLClient:
            if sasl_auth == "GSSAPI":
                sasl_client = SASLClient(host, kerberos_service_name, mechanism=sasl_auth)
            elif sasl_auth == "PLAIN":
                sasl_client = SASLClient(
                    host, mechanism=sasl_auth, username=username, password=password
                )
            else:
                raise AssertionError
            return sasl_client

        transport = thrift_sasl.TSaslClientTransport(sasl_factory, sasl_auth, socket)
    return transport


def _is_retryable_error(exc: Exception) -> str:
    message = str(exc).lower()
    if "pending" in message or "temporarily_unavailable" in message:
        return str(exc)
    else:
        return ""
