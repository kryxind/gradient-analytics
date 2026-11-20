import ssl
import unittest
from unittest.mock import Mock, patch
from urllib.error import URLError
from async_pipeline import fetch_json
from pipeline import create_ssl_context, http_json

class TLSTests(unittest.TestCase):
    def test_verified_context(self):
        context = create_ssl_context()
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertGreater(context.cert_store_stats()['x509_ca'], 0)

    def test_sync_certificate_failure_not_retried(self):
        sleeper = Mock()
        with patch('pipeline.urlopen', side_effect=URLError(ssl.SSLCertVerificationError('invalid certificate'))) as fetch:
            with self.assertRaisesRegex(RuntimeError, 'not retried'):
                http_json('https://example.com', sleeper=sleeper)
            fetch.assert_called_once()
            sleeper.assert_not_called()

class AsyncTLSTests(unittest.IsolatedAsyncioTestCase):
    async def test_certificate_failure_not_retried(self):
        session = Mock()
        session.get.side_effect = ssl.SSLCertVerificationError('invalid certificate')
        with self.assertRaisesRegex(RuntimeError, 'not retried'):
            await fetch_json(session, 'https://example.com')
        session.get.assert_called_once()
