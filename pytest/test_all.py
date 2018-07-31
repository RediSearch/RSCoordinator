import os
import sys

OSS_TEST_DIR = os.path.dirname(os.path.abspath(__file__)) + '/../'
sys.path.append(OSS_TEST_DIR)

import redisearch_pytests.base_case as oss_base_case
import rmtest2; rmtest2.set_cluster_mode()
import base_case


# del oss_base_case.BaseSearchTestCase
oss_base_case.BaseSearchTestCase = base_case.BaseSearchTestCase

from redisearch_pytests.test import SearchTestCase
from redisearch_pytests.test_aggregate import AggregateTestCase
from redisearch_pytests.test_cn import CnTestCase
from redisearch_pytests.test_conditional_updates import ConditionalUpdateTestCase
from redisearch_pytests.test_cursors import CursorTestCase
from redisearch_pytests.test_doctable import DocTableTestCase
from redisearch_pytests.test_fuzz import FuzzTestCase
from redisearch_pytests.test_fuzzy import FuzzyTestCase

# This won't work for cluster!
# from oss_tests.test_gc import SearchGCTestCase

from redisearch_pytests.test_scorers import ScorersTestCase
from redisearch_pytests.test_summarize import SummarizeTestCase
from redisearch_pytests.test_synonyms import SynonymsTestCase
from redisearch_pytests.test_tags import TagsTestCase
from redisearch_pytests.test_wideschema import WideSchemaTestCase

# Spell check
from redisearch_pytests.test_spell_check import SpellCheckTestCase