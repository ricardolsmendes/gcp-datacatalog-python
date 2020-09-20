"""
This application demonstrates how to manage Taxonomy and Policy Tags
in Google Cloud Data Catalog.
"""
import argparse
import logging
import sys

from google.cloud import datacatalog


"""
Constants
========================================
"""


_CLOUD_PLATFORM_LOCATION = 'us'


"""
Taxonomy manager
========================================
"""


class TaxonomyManager:

    def __init__(self):
        self.__datacatalog_facade = DataCatalogFacade()

    def create_taxonomy(self, project_id, display_name, description=None):
        return self.__datacatalog_facade.create_taxonomy(project_id, display_name, description)


"""
API communication classes
========================================
"""


class DataCatalogFacade:
    """
    Manage Taxonomy and Policy Tags by communicating to Data Catalog's API.
    """

    def __init__(self):
        # Initialize the API client.
        self.__datacatalog = datacatalog.PolicyTagManagerClient()

    def create_taxonomy(self, project_id, display_name, description=None):
        """Create a Taxonomy."""

        location = f'projects/{project_id}/locations/{_CLOUD_PLATFORM_LOCATION}'

        taxonomy = datacatalog.Taxonomy()
        taxonomy.display_name = display_name
        taxonomy.description = description

        created_taxonomy = self.__datacatalog.create_taxonomy(
            parent=location, taxonomy=taxonomy)

        logging.info(f'===> Taxonomy created: {created_taxonomy.name}')


"""
Command-line interface
========================================
"""


class PolicyTagsManagerCLI:

    @classmethod
    def run(cls, argv):
        cls.__setup_logging()

        args = cls._parse_args(argv)
        args.func(args)

    @classmethod
    def __setup_logging(cls):
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description='Manage Taxonomy and Policy Tags')

        subparsers = parser.add_subparsers()

        create_taxonomy_parser = subparsers.add_parser('create-taxonomy', help='Create Taxonomy')
        create_taxonomy_parser.add_argument('--display-name',
                                            help='Display name',
                                            required=True)
        create_taxonomy_parser.add_argument('--description',
                                            help='Description')
        create_taxonomy_parser.add_argument('--project-id',
                                            help='GCP Project to create the Taxonomy into',
                                            required=True)
        create_taxonomy_parser.set_defaults(func=cls.__create_taxonomy)

        return parser.parse_args(argv)

    @classmethod
    def __create_taxonomy(cls, args):
        TaxonomyManager().create_taxonomy(
            project_id=args.project_id, display_name=args.display_name,
            description=args.description)


"""
Main program entry point
========================================
"""
if __name__ == "__main__":
    PolicyTagsManagerCLI.run(sys.argv[1:] if len(sys.argv) > 0 else sys.argv)
