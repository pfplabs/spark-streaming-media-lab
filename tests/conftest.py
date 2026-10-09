import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pytest
from runtime import spark_session
@pytest.fixture(scope="session")
def spark():
    s = spark_session()
    yield s
    s.stop()
