import pytest
from pathlib import Path

from agent import AsyncRPCClient, IAgentRPCNode


# ------------------    HELPERS    -------------------------------------------


test_addr = "localhost"
path = Path(__file__).parent
test_cert_path = path / "test_cert.crt"
test_key_path = path / "test_key.key"
api_key = (path / "api_key.raw").open().read()


@pytest.fixture
async def rpc_node():
    conn = AsyncRPCClient(test_addr, test_cert_path, api_key)
    return IAgentRPCNode(test_addr, conn)


@pytest.mark.asyncio
async def test_run_ceph_cmd(rpc_node: IAgentRPCNode):
    async with rpc_node:
        ps_result = await rpc_node.run("ceph --version")
        assert ps_result.returncode == 0
        assert ps_result.stdout.strip().startswith("ceph version")


@pytest.mark.asyncio
async def test_historic_dumps(rpc_node: IAgentRPCNode):
    async with rpc_node:
        await rpc_node.conn.ceph.start_historic_collection(record_file_path="/tmp/record.bin",
                                                           osd_ids=None,
                                                           duration=10,
                                                           size=10,
                                                           pg_dump_timeout=30,
                                                           extra_dump_timeout=20,
                                                           extra_cmd=["rados df -f json", "ceph df -f json"])
        print(await rpc_node.conn.ceph.get_historic_collection_status())
