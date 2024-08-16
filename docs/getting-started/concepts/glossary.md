# Glossary

Bruin focuses on enabling independent teams designing independent data products. These products ought to be built and developed independently, while all working towards a cohesive data strategy. 

One of the most important aspects of this aligned approach to building data products is agreeing on the language. For instance, if you go to an e-commerce company and ask different individuals in different teams "what is a customer for you?", you will get different answers:
- a CRM manager might say: "a customer is someone who signed up to our email list.
- a backend engineer could say: "a customer is a row in the `customers` table in the backend.
- a marketing manager could say: "a customer is everyone that successfully finished signing up to our platform"
- a product manager could say: "a customer is someone who purchased at least one item through us"

The list can go on further, but you get the idea: different teams have the same name for different concepts, and aligning on these concepts are a crucial part of building successful data products.

In order to align on different teams on building on a shared language, Bruin has a feature called "glossary".

> [!INFO]
> Glossary is a beta feature, there may be unexpected behavior or mistakes while utilizing them in assets.

## Entities & Attributes

Glossaries in Bruin support two primary concepts at the time of writing:
- Entity: a high-level business entity that is not necessarily tied to a data asset, e.g. `Customer` or `Order`
- Attribute: the logical attributes of an entity, e.g. `ID` for a `Customer`, or `Address` for an `Order`.
  - Attributes have names, types and descriptions.

An entity can have zero or more attributes, while an attribute must always be within an entity. 

> [!INFO]
> Glossaries are primarily utilized for entities in its first version. In the future they will be used to incorporate further business concepts.


## Defining a glossary

In order to define your glossary, you need to put a file called `glossary.yml` at the root of the repo:
- The file `glossary.yml` must be at the root of the repo.
- The file must be named `glossary.yml` or `glossary.yaml`, nothing else.

Below is an example `glossary.yml` file that defines 2 entities, a `Customer` entity and an `Address` entity:
```yaml
# The `entities` key is used to define entities within the glossary, which can then be referred by different assets.
entities:  
  Customer:
    description: Customer is an individual/business that has registered on our platform.
    attributes:
      ID:
        type: integer
        description: The unique identifier of the customer in our systems.
      Email:
        type: string
        description: the e-mail address the customer used while registering on our website.
      Language:
        type: string
        description: the language the customer picked during registration.
  
  # You can define multi-line descriptions, and give further details or references for others. 
  Address:
    description: |
      An address represent a physical, real-world location that is used across various entities, such as customer or order.
      
      These addresses can be anywhere in the world, there is no country/geography limitation. 
      The addresses are not validated beforehand, therefore the addresses are not guaranteed to be real.
    attributes:
      ID:
        type: integer
        description: The unique identifier of the address in our systems.
      Street:
        type: string
        description: The given street name for the address, depending on the country it may have a different structure.
      Country:
        type: string
        description: The country of the address, represents a real country.
      CountryCode:
        type: string
        description: The ISO country code for the given country.
```

The file structure is flexible enough to allow conceptual attributes to be defined here. You can define unlimited number of entities and attributes.

### Schema
The `glossary.yml` file has a rather simple schema:
- `entities`: must be key-value pairs of string - an Entity object
- `Entity` object:
  - `description`: string, supports markdown descriptions
  - `attributes`: a key-value map, where the key is the name of the attribute, the value is an object.
    - `type`: the data type of the attribute
    - `description`: the markdown description of the given column

Take a look at the example above and modify it as per your needs.

## Utilizing entities in assets via `extends`
One of the early uses of entities is addressing & documenting concepts that repeat in multiple places. For instance, let's say you have a "age" property that is used across 5 different assets, this means you would have to go and document each of these columns one by one inside the assets. Using "entities", you can instead refer them all to a single attribute using the `extends` keyword.

```yaml
name: raw.customers

columns:
  - name: customer_id
    extends: Customer.ID
```

Let's take a look at the `extends` key here:
- The format it follows is `<entity>.<attribute>`, e.g. `Customer.ID` refers to the `ID` attribute in the `Customer` entity.
- In this example, the column `id` extends the attribute `ID` of `Customer`, which means it will take the attribute definition as the default:
  - There's already a `name` defined, `id`, that takes priority.
  - There's no `description` for the column, and the attribute has that, therefore take the `description` from the `Customer.ID` attribute.
  - There's no `type` defined for the column, therefore take the `type` from the `Customer.ID` attribute too. 


In the end, the resulting asset will behave as if it is defined this way:
```yaml
name: raw.customers

columns:
  - name: customer_id
    description: The unique identifier of the customer in our systems. # this comes from the `ID` attribute of the `Customer` entity
    type: integer # this comes from the `ID` attribute of the `Customer` entity
```

Thanks to the entities, you don't have to repeat definitions across assets.

### Order of priority
Entities are used as defaults when there are no explicit definitions on an asset column. Entity attributes support the following fields:
- `name`: the name of the attribute
- `type`: the type of the attribute
- `description`: the human-readable description of the attribute, ideally in relation to the business

Bruin will take all of the fields from the attribute, and combine them with the asset column:
- if the column definition already has a value for a field, use that.
- if not, and the entity attribute has that, use that.
- if neither has the value for the field, leave it empty.

This means, the following asset definition will still produce a valid asset:
```yaml
name: raw.customers

columns:
  - extends: Customer.ID # use `ID` as the column name, `integer` as the `type`, and `description` from the attribute
  - extends: Customer.Email # use `Email` as the column name, `string` as the `type`, and `description` from the attribute
```

Bruin will parse the `extends` references, and merge them with the corresponding attribute definitions.

> [!WARNING]
> Explicit definition of a column will always take priority over entity attributes. Attributes are there to provide defaults, not to override explicit definitions on an asset level. Asset has higher priority than the glossary.


