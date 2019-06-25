"""This application demonstrates how to perform basic operations with the
Cloud Data Catalog API.

For more information, see the README.md and the official documentation at
https://cloud.google.com/data-catalog/docs.
"""
import argparse

from google.cloud import datacatalog_v1beta1


class DataCatalogHelper:

    @staticmethod
    def __make_search_results_list(results_pages_iterator):
        return [result for result in results_pages_iterator]

    def __init__(self):
        # Initialize the API client.
        self.__datacatalog = datacatalog_v1beta1.DataCatalogClient()

    def get_entry(self, name):
        """Get the Data Catalog Entry for a given name."""

        return self.__datacatalog.get_entry(name=name)

    def lookup_entry(self, linked_resource):
        """Lookup the Data Catalog Entry for a given resource."""

        return self.__datacatalog.lookup_entry(linked_resource=linked_resource)

    def search_catalog(self, organization_id, query):
        """Search Data Catalog for a given organization."""

        scope = datacatalog_v1beta1.types.SearchCatalogRequest.Scope()
        scope.include_org_ids.append(organization_id)

        return DataCatalogHelper.__make_search_results_list(
            self.__datacatalog.search_catalog(scope=scope, query=query))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('organization_id', help='Your Google Cloud organization ID')
    parser.add_argument('project_id', help='Your Google Cloud project ID')

    args = parser.parse_args()

    datacatalog_helper = DataCatalogHelper()

    bq_datasets_search_results = datacatalog_helper.search_catalog(
        args.organization_id, 'system=bigquery type=dataset quickstart')

    print(bq_datasets_search_results)

    bq_tables_column_search_results = datacatalog_helper.search_catalog(
        args.organization_id, 'column:email')

    print(bq_tables_column_search_results)

    table_2_resource_name = \
        f'//bigquery.googleapis.com/projects/{args.project_id}'\
        f'/datasets/data_catalog_quickstart/tables/quickstart_table_2'
    table_2_search_result = next(result for result in bq_tables_column_search_results
                                 if result.linked_resource == table_2_resource_name)

    table_2_entry = datacatalog_helper.get_entry(table_2_search_result.relative_resource_name)

    print(table_2_entry)

    table_1_resource_name = \
        f'//bigquery.googleapis.com/projects/{args.project_id}' \
        f'/datasets/data_catalog_quickstart/tables/quickstart_table_1'
    table_1_entry = datacatalog_helper.lookup_entry(table_1_resource_name)

    print(table_1_entry)
