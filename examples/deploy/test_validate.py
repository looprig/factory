import unittest
from pathlib import Path

from validate import validate

HERE = Path(__file__).resolve().parent


class DeploymentChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest = (HERE / "cloud-pooled.yaml").read_text()
        cls.wiring = (HERE / "wiring" / "wiring.go").read_text()

    def test_template_passes(self):
        self.assertEqual(validate(self.manifest, self.wiring), [])

    def test_literal_secret_rejected(self):
        bad = self.manifest.replace("ports:\n            -", "env:\n            - {name: DB_PASSWORD, value: leaked}\n          ports:\n            -", 1)
        self.assertTrue(any("literal credential" in e for e in validate(bad, self.wiring)))
        self.assertIn("committed Secret resource", validate(self.manifest + "\n---\napiVersion: v1\nkind: Secret\ndata: {password: c2VjcmV0}\n", self.wiring))

    def test_public_hostlink_rejected(self):
        bad = self.manifest.replace("name: http, port: 8080", "name: hostlink, port: 8080")
        self.assertIn("public HostLink exposure", validate(bad, self.wiring))

    def test_missing_bounds_rejected(self):
        bad = self.manifest.replace('limits: {cpu: "2", memory: 8Gi}', "limits: {}")
        self.assertTrue(any("missing limits" in e for e in validate(bad, self.wiring)))
        code = self.wiring.replace("PerConnectionQueueBytes = 1 << 20", "PerConnectionQueueBytes = 0")
        self.assertTrue(any("byte queue" in e for e in validate(self.manifest, code)))

    def test_unsafe_timing_and_s3_policy_rejected(self):
        bad = self.manifest.replace("terminationGracePeriodSeconds: 120", "terminationGracePeriodSeconds: 5")
        self.assertTrue(any("termination grace" in e for e in validate(bad, self.wiring)))
        code = self.wiring.replace("RequireConfirmedEncryption: true", "RequireConfirmedEncryption: false")
        self.assertTrue(any("encryption policy" in e for e in validate(self.manifest, code)))


if __name__ == "__main__":
    unittest.main()
