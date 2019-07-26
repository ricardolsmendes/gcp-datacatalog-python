"""
This application demonstrates how to perform core operations with the
Data Catalog API.

For more information, see the README.md and the official documentation at
https://cloud.google.com/data-catalog/docs.
"""
import argparse

from google.api_core.exceptions import PermissionDenied
from google.cloud import datacatalog_v1beta1


class DataCatalogHelper:

    @staticmethod
    def __fetch_search_results(results_pages_iterator):
        return [result for result in results_pages_iterator]

    @staticmethod
    def __set_tag_field_value(field, value, primitive_type=None):
        set_primitive_field_value_functions = {
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.BOOL: DataCatalogHelper.__set_bool_field_value,
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE: DataCatalogHelper.__set_double_field_value,
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING: DataCatalogHelper.__set_string_field_value,
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.TIMESTAMP: DataCatalogHelper.__set_timestamp_field_value
        }

        if primitive_type:
            set_primitive_field_value = set_primitive_field_value_functions[primitive_type]
            set_primitive_field_value(field, value)
        else:
            DataCatalogHelper.__set_enum_field_value(field, value)

    @staticmethod
    def __set_bool_field_value(field, value):
        field.bool_value = value

    @staticmethod
    def __set_double_field_value(field, value):
        field.double_value = value

    @staticmethod
    def __set_enum_field_value(field, value):
        field.enum_value.display_name = value

    @staticmethod
    def __set_string_field_value(field, value):
        field.string_value = value

    @staticmethod
    def __set_timestamp_field_value(field, value_as_string):
        field.timestamp_value.FromJsonString(value_as_string)

    def __init__(self):
        # Initialize the API client.
        self.__datacatalog = datacatalog_v1beta1.DataCatalogClient()

    def create_tag_template(self, project_id, template_id, display_name, primitive_fields_descriptors):
        """Create a Tag Template."""

        location = self.__datacatalog.location_path(project_id, 'us-central1')

        tag_template = datacatalog_v1beta1.types.TagTemplate()
        tag_template.display_name = display_name

        for descriptor in primitive_fields_descriptors:
            tag_template.fields[descriptor['id']].type.primitive_type = descriptor['primitive_type']
            tag_template.fields[descriptor['id']].display_name = descriptor['display_name']

        return self.__datacatalog.create_tag_template(
            parent=location, tag_template_id=template_id, tag_template=tag_template)

    def get_tag_template(self, name):
        """Get the Tag Template for a given name."""

        return self.__datacatalog.get_tag_template(name=name)

    def delete_tag_template(self, name):
        """Delete a Tag Template."""

        self.__datacatalog.delete_tag_template(name=name, force=True)

    def create_tag_template_field(self, template_name, field_id, display_name, enum_values):
        """Add field to a Tag Template."""

        field = datacatalog_v1beta1.types.TagTemplateField()
        field.display_name = display_name

        for enum_value in enum_values:
            field.type.enum_type.allowed_values.add().display_name = enum_value['display_name']

        return self.__datacatalog.create_tag_template_field(
            parent=template_name, tag_template_field_id=field_id, tag_template_field=field)

    def delete_tag_template_field(self, name):
        """Delete a Tag Template field."""

        self.__datacatalog.delete_tag_template_field(name=name, force=True)

    def create_tag(self, entry, tag_template, fields_descriptors):
        """Create a Tag."""

        tag = datacatalog_v1beta1.types.Tag()
        tag.template = tag_template.name

        for descriptor in fields_descriptors:
            DataCatalogHelper.__set_tag_field_value(
                tag.fields[descriptor['id']], descriptor['value'], descriptor['primitive_type'])

        return self.__datacatalog.create_tag(parent=entry.name, tag=tag)

    def delete_tag(self, name):
        """Delete a Tag."""

        self.__datacatalog.delete_tag(name=name)

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

        return DataCatalogHelper.__fetch_search_results(
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

    # ================================================================================
    # 1. Search for BigQuery Datasets.
    # ================================================================================
    bq_datasets_search_results = datacatalog_helper.search_catalog(
        args.organization_id, 'system=bigquery type=dataset quickstart')

    print(bq_datasets_search_results)

    # ================================================================================
    # 2. Search for assets having the 'email' word in their columns metadata.
    # ================================================================================
    bq_tables_column_search_results = datacatalog_helper.search_catalog(
        args.organization_id, 'column:email')

    print(bq_tables_column_search_results)

    # ================================================================================
    # 3. Get the catalog entry for quickstart_table_2 based on search results.
    # ================================================================================
    table_2_resource_name = f'//bigquery.googleapis.com/projects/{args.project_id}'\
                            f'/datasets/data_catalog_quickstart/tables/quickstart_table_2'
    table_2_search_result = next(result for result in bq_tables_column_search_results
                                 if result.linked_resource == table_2_resource_name)

    table_2_entry = datacatalog_helper.get_entry(table_2_search_result.relative_resource_name)

    print(table_2_entry)

    # ================================================================================
    # 4. Lookup the catalog entry for quickstart_table_1.
    # ================================================================================
    table_1_resource_name = f'//bigquery.googleapis.com/projects/{args.project_id}' \
                            f'/datasets/data_catalog_quickstart/tables/quickstart_table_1'
    table_1_entry = datacatalog_helper.lookup_entry(table_1_resource_name)

    print(table_1_entry)

    # ================================================================================
    # 5. Create a tag template.
    # ================================================================================
    # Delete a Tag Template with the same name if it already exists.
    try:
        datacatalog_helper.delete_tag_template(f'projects/{args.project_id}/locations/us-central1/'
                                               f'tagTemplates/quickstart_classification_template')
    except PermissionDenied:
        pass

    template = datacatalog_helper.create_tag_template(
        project_id=args.project_id,
        template_id='quickstart_classification_template',
        display_name='A Tag Template to be used in the hands-on guide',
        primitive_fields_descriptors=[
            {
                'id': 'has_pii',
                'primitive_type': datacatalog_v1beta1.enums.FieldType.PrimitiveType.BOOL,
                'display_name': 'Has PII'
            }
        ]
    )

    print(template)

    # ================================================================================
    # 6. Add a field to the tag template.
    # ================================================================================
    datacatalog_helper.create_tag_template_field(
        template_name=template.name,
        field_id='pii_type',
        display_name='PII Type',
        enum_values=[
            {'display_name': 'EMAIL'},
            {'display_name': 'SOCIAL SECURITY NUMBER'}
        ]
    )

    template = datacatalog_helper.get_tag_template(template.name)
    print(template)

    # ================================================================================
    # 7. Create a tag to quickstart_table_1 catalog entry.
    # ================================================================================
    tag_entry_table_1 = datacatalog_helper.create_tag(
        entry=table_1_entry,
        tag_template=template,
        fields_descriptors=[
            {
                'id': 'has_pii',
                'primitive_type': datacatalog_v1beta1.enums.FieldType.PrimitiveType.BOOL,
                'value': False
            }
        ]
    )

    print(tag_entry_table_1)

    # ================================================================================
    # 8. Create a tag to quickstart_table_2 catalog entry.
    # ================================================================================
    tag_entry_table_2 = datacatalog_helper.create_tag(
        entry=table_2_entry,
        tag_template=template,
        fields_descriptors=[
            {
                'id': 'has_pii',
                'primitive_type': datacatalog_v1beta1.enums.FieldType.PrimitiveType.BOOL,
                'value': True
            },
            {
                'id': 'pii_type',
                'primitive_type': None,
                'value': 'EMAIL'
            }
        ]
    )

    print(tag_entry_table_2)

    # ================================================================================
    # 9. Search for assets tagged with the tag template.
    # ================================================================================
    tag_template_search_results = datacatalog_helper.search_catalog(
        args.organization_id, 'tag:quickstart_classification_template')

    print(tag_template_search_results)

    # ================================================================================
    # 10. Search for assets tagged with a given value.
    # ================================================================================
    tag_value_search_results = datacatalog_helper.search_catalog(
        args.organization_id, 'tag:quickstart_classification_template.has_pii=True')

    print(tag_value_search_results)
