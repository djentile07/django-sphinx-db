from django.db.backends.mysql.base import DatabaseWrapper as MySQLDatabaseWrapper
from django.db.backends.mysql.base import DatabaseOperations as MySQLDatabaseOperations
from django.db.backends.mysql.creation import DatabaseCreation as MySQLDatabaseCreation
from django.db.backends.base.validation import BaseDatabaseValidation

class SphinxOperations(MySQLDatabaseOperations):
    compiler_module = "django_sphinx_db.backend.sphinx.compiler"

    def fulltext_search_sql(self, field_name):
        return 'MATCH (%s)'


class SphinxCreation(MySQLDatabaseCreation):
    def create_test_db(self, **kwargs):
        return super(SphinxCreation, self).check(**kwargs)

    def destroy_test_db(self, old_database_name, **kwargs):
        # NOOP, we created nothing, nothing to destroy.
        return

class SphinxValidation(BaseDatabaseValidation):
    def check(self, **kwargs):
        return super(SphinxValidation, self).check(**kwargs)
        

class DatabaseWrapper(MySQLDatabaseWrapper):
    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.ops = SphinxOperations(self)
        self.creation = SphinxCreation(self)
        self.validation = SphinxValidation(self)
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
