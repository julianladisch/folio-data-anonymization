import pytest

from faker import Faker
import re

from folio_data_anonymization.plugins.providers import Organizations, Users


@pytest.fixture
def custom_providers():
    faker = Faker()
    faker.add_provider(Organizations)
    faker.add_provider(Users)
    return faker


def test_edi_type(custom_providers):
    fake_edi_type = custom_providers.edi_type()
    assert isinstance(fake_edi_type, str)
    assert len(fake_edi_type) >= 5
    assert fake_edi_type[3] == "/"
    assert fake_edi_type[-1].isalpha()
    assert fake_edi_type[-4] == "-"


def test_org_code(custom_providers):
    fake_org_code = custom_providers.org_code()
    assert isinstance(fake_org_code, str)
    assert len(fake_org_code) >= 3
    assert all(c.isupper() or c == "-" for c in fake_org_code)


def test_pronouns(custom_providers):
    fake_pronoun = custom_providers.pronouns()
    assert isinstance(fake_pronoun, str)


def test_username(custom_providers):
    fake_username = custom_providers.username()
    pattern = re.compile("\\w.+\\d[!#$?]\\w.+$")
    assert isinstance(pattern.search(fake_username), re.Match)
