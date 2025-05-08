#!/usr/bin/env nix-shell
#!nix-shell -i "python3 -i" -p "python311.withPackages(ps: [ ps.faker ps.flask ps.pydantic ])"

from faker import Faker
from faker.providers import BaseProvider
from faker.providers import phone_number # https://faker.readthedocs.io/en/stable/providers/faker.providers.phone_number.html
from faker.providers import person # https://faker.readthedocs.io/en/stable/providers/faker.providers.person.html
from faker.providers import internet # https://faker.readthedocs.io/en/stable/providers/faker.providers.internet.html

from pydantic import BaseModel, Field

# from antithesis.random import random_choice
from random import choice as random_choice


"""
Experimenting with custom provider in faker that integrate randomness from Antithesis
"""
class ContactProvider(BaseProvider):

    def generate_contact(self):

        self.fake = Faker()
        self.fake.add_provider(phone_number)
        self.fake.add_provider(person)
        self.fake.add_provider(internet)

        # Sometimes keep the email field blank
        email = random_choice([None, self.fake.ascii_email()])

        """
        @todo:
        1. Sometimes randomly generate a blank field
        2. Sometimes generate a bad field (give an example of bad or a range of bad) (done)
        3. Sometimes switches the localization (later because some providers are not supported in some locality)
        """
        return {
            'given_name': self.fake.first_name(),
            'family_name': self.fake.last_name(),
            'email': email,
            'phone': self.fake.phone_number(),
        }

# class ContactValidator(BaseModel):
#     given_name: str = Field(max_length=32)
#     family_name: str = Field(max_length=32)
#     email: str
#     """
#     Valid phone numbers
#     (460)648-7647x5938
#     (319)748-9241
#     281.256.5938x7784
#     560-597-5351
#     328.671.1587
#     """
#     phone: str = Field(pattern=r'(?:\(\d{3}\)|\d{3})[.-]?\d{3}[.-]\d{4}(?:x\d+)?')

# if __name__ == '__main__':
#     fake = Faker()
#     fake.add_provider(ContactProvider)

#     contact = fake.generate_contact()

#     try:
#         validate = ContactValidator(**contact)
#     except ValidationError as e:
#         print(contact)
#         # Do some assertion for correctness here

