import unittest
from unittest.mock import patch
import io
import json
import valcli


class TestValcliCentral(unittest.TestCase):
    def setUp(self):
        self.mock_cql_data = {}

    @patch("sys.stdout", new_callable=io.StringIO)
    @patch("valcli.run_cql_query")
    def test_central_status_not_deployed(self, mock_cql, mock_stdout):
        mock_cql.return_value = (0, "", "")
        valcli.execute_command(["central.status"], prog_name="ncli")
        output = mock_stdout.getvalue()
        self.assertIn("HELIOS CENTRAL APPLIANCE STATUS", output)
        self.assertIn("NOT DEPLOYED", output)
        self.assertIn("tiny (2 vCPU, 4GB RAM)", output)

    @patch("sys.stdout", new_callable=io.StringIO)
    @patch("valcli.run_cql_query")
    def test_central_deploy_dry_run(self, mock_cql, mock_stdout):
        valcli.execute_command([
            "central.deploy",
            "--name", "helios-central-test",
            "--ip", "10.10.102.60/24",
            "--preset", "tiny",
            "--dry-run"
        ], prog_name="ncli")
        output = mock_stdout.getvalue()
        self.assertIn("HELIOS CENTRAL 1-CLICK APPLIANCE DEPLOYMENT", output)
        self.assertIn("helios-central-test", output)
        self.assertIn("TINY (2 vCPU, 4096 MiB RAM, 32 GiB Sidon CoW Disk)", output)
        self.assertIn("[DRY RUN]", output)

    @patch("sys.stdout", new_callable=io.StringIO)
    @patch("valcli.run_mtls_api")
    @patch("valcli.run_spectrum_api")
    @patch("valcli.run_cql_query")
    def test_central_deploy_execute(self, mock_cql, mock_spectrum, mock_mtls, mock_stdout):
        mock_cql.return_value = (0, "", "")
        mock_spectrum.return_value = (0, {"message": "VM created"})
        mock_mtls.return_value = (0, {"message": "VM powered on"}, "")
        valcli.execute_command([
            "central.deploy",
            "--name", "helios-central-prod",
            "--ip", "10.10.102.50/24",
            "--preset", "standard"
        ], prog_name="ncli")
        output = mock_stdout.getvalue()
        self.assertIn("STANDARD (4 vCPU, 8192 MiB RAM, 64 GiB Sidon CoW Disk)", output)
        self.assertIn("Step 1/4: Synthesizing Cloud-Init", output)
        self.assertIn("Step 2/4: Provisioning Sidon extent storage", output)
        self.assertIn("Step 3/4: Starting appliance domain", output)
        self.assertIn("Step 4/4: Registering cluster", output)
        self.assertIn("Success: Helios Central Virtual Appliance", output)

    @patch("sys.stdout", new_callable=io.StringIO)
    @patch("valcli.run_cql_query")
    def test_central_cluster_lifecycle(self, mock_cql, mock_stdout):
        mock_cql.return_value = (0, "", "")
        # Add cluster
        valcli.execute_command([
            "central.cluster.add",
            "cluster-beta",
            "Beta-Datacenter",
            "10.10.103.45",
            "--port", "8443",
            "--token", "secret-token-123"
        ], prog_name="ncli")
        output = mock_stdout.getvalue()
        self.assertIn("enrolled into Helios Central fleet", output)

        # List clusters
        mock_cql.return_value = (0, '{"cluster_id": "cluster-beta", "name": "Beta-Datacenter", "vip": "10.10.103.45", "port": 8443, "node_count": 3, "vm_count": 5, "status": "CONNECTED"}', "")
        mock_stdout.truncate(0)
        mock_stdout.seek(0)
        valcli.execute_command(["central.cluster.list"], prog_name="ncli")
        list_output = mock_stdout.getvalue()
        self.assertIn("cluster-beta", list_output)
        self.assertIn("Beta-Datacenter", list_output)

        # Remove cluster
        mock_cql.return_value = (0, "", "")
        mock_stdout.truncate(0)
        mock_stdout.seek(0)
        valcli.execute_command(["central.cluster.remove", "cluster-beta"], prog_name="ncli")
        remove_output = mock_stdout.getvalue()
        self.assertIn("removed from Helios Central fleet", remove_output)


if __name__ == "__main__":
    unittest.main()
