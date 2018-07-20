import rmtest2; rmtest2.set_cluster_mode()
import base_case
from oss_tests import base_case as oss_base_case
# del oss_base_case.BaseSearchTestCase
oss_base_case.BaseSearchTestCase = base_case.BaseSearchTestCase

from oss_tests.test import SearchTestCase
from oss_tests.test_aggregate import AggregateTestCase
from oss_tests.test_cn import CnTestCase
from oss_tests.test_conditional_updates import ConditionalUpdateTestCase
from oss_tests.test_cursors import CursorTestCase
from oss_tests.test_doctable import DocTableTestCase
from oss_tests.test_fuzz import FuzzTestCase
from oss_tests.test_fuzzy import FuzzyTestCase

# This won't work for cluster!
# from oss_tests.test_gc import SearchGCTestCase

from oss_tests.test_scorers import ScorersTestCase
from oss_tests.test_summarize import SummarizeTestCase
from oss_tests.test_synonyms import SynonymsTestCase
from oss_tests.test_tags import TagsTestCase
from oss_tests.test_wideschema import WideSchemaTestCase