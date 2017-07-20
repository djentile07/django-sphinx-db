from django.db.backends.mysql.base import DatabaseWrapper as MySQLDatabaseWrapper
from django.db.backends.mysql.base import DatabaseOperations as MySQLDatabaseOperations
from django.db.backends.mysql.creation import DatabaseCreation as MySQLDatabaseCreation
from django.db.backends.base.validation import BaseDatabaseValidation
from django.db.backends.mysql.introspection import BaseDatabaseIntrospection
from django.db.backends.mysql.schema import DatabaseSchemaEditor

class SphinxOperations(MySQLDatabaseOperations):
    compiler_module = "django_sphinx_db.backend.sphinx.compiler"

    def fulltext_search_sql(self, field_name):
        return 'MATCH (%s)'


class SphinxCreation(MySQLDatabaseCreation):
    def create_test_db(self, **kwargs):
        #return super(SphinxCreation, self).create_test_db(**kwargs)
        return

    def destroy_test_db(self, *args, **kwargs):
        # NOOP, we created nothing, nothing to destroy.
        return

class SphinxValidation(BaseDatabaseValidation):
    def check(self, **kwargs):
        #return super(SphinxValidation, self).check(**kwargs)
        return []

    def check_field_type(self, field, field_type):
        return []
        
class SphinxIntrospection(BaseDatabaseIntrospection):
    def table_names(self, cursor=None, include_views=False):
        return []

class SphinxDatabaseSchemaEditor(DatabaseSchemaEditor):
    def create_model(self, model):
        """
        This is to try to make it read only
        """
        return

class DatabaseWrapper(MySQLDatabaseWrapper):
    SchemaEditorClass = SphinxDatabaseSchemaEditor
    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.ops = SphinxOperations(self)
        self.creation = SphinxCreation(self)
        self.validation = SphinxValidation(self)
        self.introspection = SphinxIntrospection(self)
        # The following can be useful for unit testing, with multiple databases
        # configured in Django, if one of them does not support transactions,
        # Django will fall back to using clear/create (instead of begin...rollback)
        # between each test. The method Django uses to detect transactions uses
        # CREATE TABLE and DROP TABLE, which ARE NOT supported by Sphinx, even though
        # transactions ARE. Therefore, we can just set this to True, and Django will
        # use transactions for clearing data between tests when all OTHER backends
        # support it.
        self.features.supports_transactions = True
        self.features.is_sql_auto_is_null_enabled = False
