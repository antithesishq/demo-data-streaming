#!/usr/bin/env nix-shell
#!nix-shell -i "python3 -i" -p "python311.withPackages(ps: [ ps.faker ps.flask ps.pydantic ])"

from faker import Faker
from faker.providers import BaseProvider
from faker.providers import phone_number # https://faker.readthedocs.io/en/stable/providers/faker.providers.phone_number.html
from faker.providers import person # https://faker.readthedocs.io/en/stable/providers/faker.providers.person.html
from faker.providers import internet # https://faker.readthedocs.io/en/stable/providers/faker.providers.internet.html
from faker.providers import python

from pydantic import BaseModel, Field

from antithesis.random import random_choice
# from random import choice as random_choice
from antithesis.random import get_random
# from random import getrandbits as get_random



"""
Experimenting with custom provider in faker that integrate randomness from Antithesis
"""
class ContactProvider(BaseProvider):

    def generate_contact(self):

        self.fake = Faker()
        self.fake.add_provider(phone_number)
        self.fake.add_provider(person)
        self.fake.add_provider(internet)
        self.fake.add_provider(python)

        def generate_local():
            local = f'{self.fake.pystr(10,10)}'
            print(local)
            invalid_chars = ['!', '#', '$', '%', '^', '&', '*', '=', '{', '}', '[', ']', '|', '\\', '/', ':', ';', '<', '>', ',', '`', '~']

            # generate a violation of the local 10% of the time
            if get_random() < (1 << 64) // 10:
                num_violations = get_random() % 3 + 1
                print(f"local violations: {num_violations}")
                for _ in range(num_violations):
                    violations = [
                        f"{local} {self.fake.pystr(1,5)}",              # space
                        f"{local} ({self.fake.pystr(1,5)})",            # parentheses without quotes
                        f"{local}..{self.fake.pystr(1,5)}",             # consecutive dots
                        f'{local}"',                                    # dangling quote
                        f'"{local}',                                    # partial quote
                        f'{local}{self.fake.pystr(64, 64)}',            # too long
                        f'{local}{random_choice(invalid_chars)}'        # invalid chars
                    ]

                    local = random_choice(violations)
                    print(local)

            return local

        def generate_domain():
            domain = random_choice([self.fake.free_email_domain(), self.fake.domain_name(get_random() % 4 + 1)])
            print(domain)

            if get_random() < (1 << 64) // 10:
                num_violations = get_random() % 3 + 1
                print(f"domain violations: {num_violations}")
                for _ in range(num_violations):
                    violations = [
                        f"{domain}_bad",                         # underscore
                        f"-{domain}",                            # starts with hyphen
                        f"{domain}-",                            # ends with hyphen
                        f"{domain}..{self.fake.pystr(1,5)}",     # consecutive dots
                        f".{domain}",                            # starts with dot
                        f"{self.fake.pystr(255,255)}{domain}",   # too long
                        f"{self.fake.pystr(64,64)}.{domain}"     # subdomain too long
                    ]
                    domain = random_choice(violations)
                    print(domain)

            return domain

        def generate_email():
            email = ""
            if get_random() < (1 << 64) // 100:
                violations = [
                    f"{generate_local()}{generate_domain()}",   # missing @
                    f"{generate_local()}@",                     # missing domain
                    f"@{generate_domain()}",                    # missing local
                    ""                                          # no email
                ]
                email = random_choice(violations)
                print("email structure violation")
            else:  
                email = f"{generate_local()}@{generate_domain()}"

            return email

        """
        @todo:
        1. Sometimes randomly generate a blank field
        2. Sometimes generate a bad field (give an example of bad or a range of bad) (done)
        3. Sometimes switches the localization (later because some providers are not supported in some locality)
        """
        return {
            'given_name': self.fake.first_name(),
            'family_name': self.fake.last_name(),
            'email': generate_email(),
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