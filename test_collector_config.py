"""Startup configuration checks; no credentials, fixtures or network required."""
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

from collector import REQUIRED_ENVIRONMENT, missing_environment


class ConfigurationTests(unittest.TestCase):
    def test_reports_every_missing_or_blank_setting(self):
        settings={key:'configured' for key in REQUIRED_ENVIRONMENT}
        self.assertEqual(missing_environment(settings),[])
        del settings['SPACES_BUCKET'];settings['PUMP_RUN_ID']='  '
        self.assertEqual(missing_environment(settings),['SPACES_BUCKET','PUMP_RUN_ID'])

    def test_process_exits_before_network_and_never_prints_secret_values(self):
        environment={k:v for k,v in os.environ.items() if k not in REQUIRED_ENVIRONMENT}
        secret='synthetic-secret-must-not-appear'
        environment.update(HELIUS=secret,SPACES_ACCESS_KEY_ID=secret,SPACES_SECRET_ACCESS_KEY=secret)
        run=subprocess.run([sys.executable,'collector.py'],cwd=Path(__file__).parent,
            env=environment,text=True,capture_output=True,timeout=10)
        self.assertEqual(run.returncode,1)
        self.assertNotIn(secret,run.stdout+run.stderr)
        self.assertEqual(run.stderr,'')
        record=json.loads(run.stdout)
        self.assertEqual(record['error_type'],'MissingEnvironment')
        self.assertEqual(record['missing_environment_variables'],['SPACES_BUCKET','SPACES_REGION','PUMP_RUN_ID'])


if __name__=='__main__':unittest.main()
