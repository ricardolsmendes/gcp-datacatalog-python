"""This application demonstrates how to perform basic operations with the
Cloud Data Catalog API.

For more information, see the README.md and the official documentation at
https://cloud.google.com/data-catalog/docs.
"""
import argparse

from google.cloud import datacatalog_v1beta1

# Instantiates the API client.
datacatalog = datacatalog_v1beta1.DataCatalogClient()


# Catalog search
def search_catalog(organization_id, query):
    """Searches Data Catalog entries for a given organization."""

    scope = datacatalog_v1beta1.types.SearchCatalogRequest.Scope()
    scope.include_org_ids.append(organization_id)

    results_iterator = datacatalog.search_catalog(scope=scope, query=query)
    return [result for result in results_iterator]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('organization_id', help='Your Google Cloud organization ID')
    parser.add_argument('project_id', help='Your Google Cloud project ID')

    args = parser.parse_args()
