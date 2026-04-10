"""Push query SQL from the repo to Dune, organized by dashboard."""
import os
import sys
import argparse
import codecs
import yaml
from dune_client.client import DuneClient
from dotenv import load_dotenv

sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

ROOT = os.path.join(os.path.dirname(__file__), '..')
load_dotenv(os.path.join(ROOT, '.env'))

dune = DuneClient.from_env()


def load_dashboards(dashboard_filter=None):
    """Load dashboards.yml, optionally filtering to one dashboard."""
    path = os.path.join(ROOT, 'dashboards.yml')
    if not os.path.exists(path):
        # Fallback to legacy queries.yml
        legacy = os.path.join(ROOT, 'queries.yml')
        with open(legacy, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return {'legacy': {'query_ids': data['query_ids']}}

    with open(path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)

    dashboards = data.get('dashboards', {})
    if dashboard_filter:
        if dashboard_filter not in dashboards:
            print(f'ERROR: dashboard "{dashboard_filter}" not found. Available: {list(dashboards.keys())}')
            sys.exit(1)
        return {dashboard_filter: dashboards[dashboard_filter]}
    return dashboards


def find_query_file(query_id, dashboards):
    """Find the SQL file for a query ID across all dashboard folders."""
    for name in dashboards:
        queries_dir = os.path.join(ROOT, 'queries', name)
        if not os.path.isdir(queries_dir):
            continue
        for f in os.listdir(queries_dir):
            if str(query_id) == f.split('___')[-1].split('.')[0]:
                return os.path.join(queries_dir, f)
    return None


def push_query(query_id, dashboard_name, all_dashboards, pushed_ids):
    """Push a single query from disk to Dune."""
    if query_id in pushed_ids:
        print(f'  SKIP: query {query_id} already pushed (shared query)')
        return
    pushed_ids.add(query_id)

    filepath = find_query_file(query_id, all_dashboards)
    if filepath is None:
        print(f'  ERROR: no SQL file found for query {query_id}')
        return

    query = dune.get_query(query_id)
    print(f'  PROCESSING: query {query_id}, {query.base.name}')

    with open(filepath, 'r', encoding='utf-8') as f:
        text = f.read()

    dune.update_query(query.base.query_id, query_sql=text)
    print(f'  SUCCESS: pushed query {query_id} from {os.path.relpath(filepath, ROOT)}')


def main():
    parser = argparse.ArgumentParser(description='Push queries from the repo to Dune')
    parser.add_argument('--dashboard', '-d', help='Push only this dashboard (default: all)')
    args = parser.parse_args()

    all_dashboards = load_dashboards()  # always load all for file lookup
    target_dashboards = load_dashboards(args.dashboard)
    pushed_ids = set()

    for name, config in target_dashboards.items():
        query_ids = config.get('query_ids', [])
        print(f'\n=== Dashboard: {name} ({len(query_ids)} queries) ===')
        for qid in query_ids:
            push_query(qid, name, all_dashboards, pushed_ids)

    print(f'\nDone. Pushed {len(pushed_ids)} unique queries.')


if __name__ == '__main__':
    main()
