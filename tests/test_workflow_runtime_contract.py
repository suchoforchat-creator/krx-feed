"""The same pinned runtime must test and collect, without changing schedules."""
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_production_runtime_and_schedule_contract():
    workflow = yaml.load((ROOT / '.github/workflows/data-pipeline.yml').read_text(encoding='utf-8'),
                         Loader=yaml.BaseLoader)
    assert [item['cron'] for item in workflow['on']['schedule']] == ['30 22 * * 0-4', '00 08 * * 1-5']
    assert set(workflow['on']) == {'schedule', 'workflow_dispatch'}
    assert workflow['permissions'] == {'contents': 'write'}
    assert workflow['concurrency'] == {'group': 'data-pipeline', 'cancel-in-progress': 'false'}
    assert set(workflow['jobs']) == {'collect_0730', 'finalize_1700'}
    for job in workflow['jobs'].values():
        assert job['runs-on'] == 'ubuntu-26.04'
        steps = job['steps']
        python_setup = [s for s in steps if s.get('uses') == 'actions/setup-python@v5']
        assert len(python_setup) == 1
        assert python_setup[0]['with']['python-version'] == '3.11.18'
        installs = [i for i, s in enumerate(steps) if s.get('run', '').startswith('pip install')]
        assert len(installs) == 1
        assert '-c constraints-runtime.txt' in steps[installs[0]]['run']
        runtime = [i for i, s in enumerate(steps) if 'tools/runtime_evidence.py' in s.get('run', '')]
        tests = [i for i, s in enumerate(steps) if '-m pytest' in s.get('run', '')]
        collection = [i for i, s in enumerate(steps) if 'python pipeline.py ' in s.get('run', '')]
        assert len(runtime) == len(tests) == len(collection) == 1
        assert installs[0] < runtime[0] < tests[0] < collection[0]
        assert '--require-python 3.11.18' in steps[runtime[0]]['run']
        artifacts = [s for s in steps if s.get('uses') == 'actions/upload-artifact@v4']
        assert len(artifacts) == 1
        assert artifacts[0]['if'] == 'always()'
        assert 'debug/runtime.json' in artifacts[0]['with']['path']


def test_constraint_snapshot_does_not_upgrade_away_failure():
    constraints = (ROOT / 'constraints-runtime.txt').read_text(encoding='utf-8')
    assert 'pandas==2.3.3' in constraints
    assert 'numpy==2.4.6' in constraints
    assert 'pytz==2026.6' in constraints
    assert 'tzdata==2026.6' in constraints
    assert 'python-dateutil==4.0.0' in constraints
    assert 'yfinance==1.8.0' in constraints
