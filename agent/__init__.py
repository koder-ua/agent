MAX_FILE_SIZE = 1 << 30
USER_NAME = 'rpc_client'
DEFAULT_PORT = 55667


from .client import AsyncRPCClient, ConnectionClosed, IAgentRPCNode, ConnectionPool
