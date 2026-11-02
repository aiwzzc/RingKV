import pathlib
import subprocess
import sys
import tempfile

binary = sys.argv[1]


def run(*args, success=True, contains=None):
    result = subprocess.run([binary, *args], text=True, capture_output=True, timeout=5)
    assert (result.returncode == 0) == success, result
    if contains:
        assert contains in result.stdout + result.stderr, result
    return result.stdout


run("--help", contains="Usage:")
run("--check-config", contains="Configuration OK")
run("--dump-config", contains="shard_threads 1")
run("--config", "/no/such/ringkv.conf", "--check-config", success=False, contains="cannot open")
for args in [("--port", "0"), ("--threads", "-1"), ("--threads", "257"),
             ("--port", "6379x"), ("--ip", "bad"), ("--unknown",),
             ("--port",), ("--port", "1", "--port", "2")]:
    run(*args, "--check-config", success=False)

with tempfile.TemporaryDirectory() as directory:
    config = pathlib.Path(directory) / "server.conf"
    config.write_text("  # comment\nport 6000\nshard_threads 2 # inline comment\n")
    output = run("--config", str(config), "--port", "6001", "--dump-config")
    assert "port 6001\n" in output and "shard_threads 2\n" in output
    # Effective config is itself a valid input file.
    config.write_text(output)
    run("--config", str(config), "--check-config")
    for text in ["port 1\nport 2\n", "porrt 6000\n", "port\n", "port 1 extra\n",
                 "aof_enabled maybe\n", "aof_sync sometimes\n", "rdb_interval -1\n",
                 "aof_enabled yes\n", "rdb_enabled yes\n", "cluster_enabled yes\n",
                 "is_master no\n"]:
        config.write_text(text)
        run("--config", str(config), "--check-config", success=False)

print("config CLI tests passed")
