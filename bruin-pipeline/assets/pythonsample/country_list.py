""" @bruin

name: myschema.country_list
depends:
  - myschema.example

description: |
  # Sample Python asset
  This file will be executed as is, it can import other Python modules, install any packages, etc.

  You can define columns and custom checks that can be executed in the same way as SQL assets, as long as the asset name matches the table name.
  
  - For the dependencies to be installed, Bruin will find the closes requirements.txt file and install the dependencies there in isolated environments.
  - Bruin will execute the script, and then run all the quality checks afterwards.

columns:
  - name: id
    type: integer
    description: "Just a number"
    primary_key: true
    checks:
        - name: not_null
        - name: positive
        - name: non_negative

  - name: country
    type: varchar
    description: "the country"
    primary_key: true
    checks:
        - name: not_null

  - name: name
    type: varchar
    update_on_merge: true
    description: "Just a name"
    checks:
        - name: unique
        - name: not_null

@bruin """

import cowsay 

cowsay.cow('Hello World')