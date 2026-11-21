import argparse
import pathlib
import shlex
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'infra/vps'))
from provision import Hosts, inventory
from vps_config import NAMESPACE


def main():
    p = argparse.ArgumentParser(description='Run service-local P5 store regressions in isolated PostgreSQL schemas on the VPS')
    p.add_argument('--binary', type=pathlib.Path, required=True)
    p.add_argument('--service', choices=['notifier', 'aggregator'], default='notifier')
    args = p.parse_args()
    h = Hosts(inventory(ROOT / 'infra/vps/inventory.json'))
    target = '/tmp/aat-notifier-store-' + str(time.time_ns()) + '.test'
    folder = ROOT / '.e2e' / ('p5-'+args.service+'-store-'+str(time.time_ns()))
    folder.mkdir()
    base = ['sudo', '-n', 'k3s', 'kubectl', '-n', NAMESPACE, 'exec', '-i', 'aggregator-coordinator-0', '--', 'sh', '-ec']
    def run(script, **kw):
        return subprocess.run(h.command(h.nodes[0], shlex.join(base + [script]), multiplex=False), check=kw.pop('check', True), timeout=180, **kw)
    try:
        run('umask 077; dd of='+target+' bs=1048576 status=none; chmod 700 '+target, input=args.binary.read_bytes())
        variable = 'NOTIFIER_TEST_DATABASE_URL' if args.service == 'notifier' else 'P2_TEST_DATABASE_URL'
        pattern = 'TestPostgresConcurrentReceiversAndLegacyImport' if args.service == 'notifier' else 'TestPostgres(CriticalProjectionBypassesGeneralFailureAndReplaysSafely|SourceRetrySurvivesOwnerChange|AdmissionFingerprintSurvivesTimestampRoundTrip)'
        result = run(variable+'="postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@127.0.0.1:5432/$POSTGRES_DB" '+target+
            ' -test.v -test.timeout=120s -test.run='+shlex.quote(pattern), capture_output=True, check=False)
        (folder/'stdout.log').write_bytes(result.stdout)
        (folder/'stderr.log').write_bytes(result.stderr)
        print(result.stdout.decode(), flush=True)
        print('Evidence:', folder.relative_to(ROOT), flush=True)
        result.check_returncode()
    finally:
        run('rm -f -- '+target)


if __name__ == '__main__':
    main()
