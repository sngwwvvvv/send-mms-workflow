import tempfile
import unittest
from pathlib import Path
from sens_mms.config import ConfigError, load_config

VALUES = {
    'NCP_ACCESS_KEY_ID':'access','NCP_SECRET_KEY':'secret','NCP_SENS_SERVICE_ID':'svc',
    'NCP_SENS_FROM_NUMBER':'0212345678','SENS_CONTENT_TYPE':'COMM'}

class ConfigTests(unittest.TestCase):
    def write_env(self, values):
        d = Path(tempfile.mkdtemp()); p=d/'.env'; p.write_text('\n'.join(f'{k}={v}' for k,v in values.items()), encoding='utf-8'); return d,p
    def test_process_overrides_file(self):
        d,p=self.write_env(VALUES|{'NCP_ACCESS_KEY_ID':'file'}); c=load_config(d, {'NCP_ACCESS_KEY_ID':'process'}, p); self.assertEqual(c.access_key,'process')
    def test_loads_missing_from_file(self):
        d,p=self.write_env(VALUES); c=load_config(d, {}, p); self.assertEqual(c.content_type,'COMM')
    def test_missing_key_does_not_leak(self):
        d,p=self.write_env({k:v for k,v in VALUES.items() if k!='NCP_SECRET_KEY'})
        with self.assertRaisesRegex(ConfigError,'NCP_SECRET_KEY') as e: load_config(d, {}, p)
        self.assertNotIn('secret',str(e.exception))
    def test_ad_rejected(self):
        d,p=self.write_env(VALUES|{'SENS_CONTENT_TYPE':'AD'})
        with self.assertRaisesRegex(ConfigError,'SENS_CONTENT_TYPE'): load_config(d, {}, p)
