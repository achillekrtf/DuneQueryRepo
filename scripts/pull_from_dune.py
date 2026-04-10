"""Pull query SQL from Dune into the repo, organized by dashboard."""
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


def pull_query(query_id, dashboard_name, seen_ids):
    """Pull a single query from Dune and write to disk."""
    if query_id in seen_ids:
        src = seen_ids[query_id]
        print(f'  SKIP: query {query_id} already pulled for {src} (shared query)')
        return
    seen_ids[query_id] = dashboard_name

    query = dune.get_query(query_id)
    name = query.base.name
    print(f'  PROCESSING: query {query_id}, {name}')

    queries_dir = os.path.join(ROOT, 'queries', dashboard_name)
    os.makedirs(queries_dir, exist_ok=True)

    # Check if file already exists for this query ID
    existing = [f for f in os.listdir(queries_dir) if str(query_id) == f.split('___')[-1].split('.')[0]]

    safe_name = name.replace(' ', '_').lower()[:30]
    filename = existing[0] if existing else f'{safe_name}___{query_id}.sql'
    filepath = os.path.join(queries_dir, filename)

    header = f'-- part of a query repo\n-- query name: {name}\n-- query link: https://dune.com/queries/{query_id}\n\n\n'

    with open(filepath, 'w', encoding='utf-8') as f:
        if '-- part of a query repo' in query.sql:
            f.write(query.sql)
        else:
            f.write(header + query.sql)

    action = 'UPDATE' if existing else 'CREATE'
    print(f'  {action}: {dashboard_name}/{filename}')


def main():
    parser = argparse.ArgumentParser(description='Pull queries from Dune into the repo')
    parser.add_argument('--dashboard', '-d', help='Pull only this dashboard (default: all)')
    args = parser.parse_args()

    dashboards = load_dashboards(args.dashboard)
    seen_ids = {}  # track shared queries across dashboards

    for name, config in dashboards.items():
        query_ids = config.get('query_ids', [])
        print(f'\n=== Dashboard: {name} ({len(query_ids)} queries) ===')
        for qid in query_ids:
            pull_query(qid, name, seen_ids)

    print(f'\nDone. Pulled {len(seen_ids)} unique queries.')


if __name__ == '__main__':
    main()
