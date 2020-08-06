# Afas consume

This functions consumes Pub/Sub messages from an Afas instance and is based on configuration.

## Configuration
A `config.py` file is used to determine what the function should do with the incoming message. 
See [config.py.example](config.py.example) for an example. 

### Data Selector
A special Data Selector specifies which fields have to be processed. Each item specifies an entity to be updated 
through Pub/Sub messages. The configuration to use is specified by the `DATA_SELECTOR` environment variable.

#### Entity name
The `entity_name` specifies the DataStore Entity kind to be created/updated.

#### ID property
When `id_property` is specified, the entity is retrieved based on key retrieval, with the value set to the specified attribute.
The entity will be created when it does not exist.

#### Filter property
When `filter_property` is specified, the entity is retrieved based on a filter on the specified attribute, comparing to
the value retrieved from the message received. Using `filter_property` only supports updated and won't create new entities.

If you want the function to create a new entity when the filtered property is not existing, you can provide the 
`create_entity` attribute. This ensures each message will result in an entity based on the `filter_property`.

#### Value formatter
When `value_formatter` is specified, the entity retrieved by the specified property can be formatted to you special needs.
The following types are supported for each value:

##### Split
A value can be split based on a character. The fields needed for this format are:
~~~python
{
    "type": "split",
    "value": ".",
    "index": -1  # Defaults to 0 when not specified
}
~~~

##### Prepend/append
A custom value can be added to the beginning (`prepend`) or the end (`append`) of a property value. 
The fields needed for this format are:
~~~python
{
    "type": "append",  # Either 'prepend' or 'append'
    "value": ".append"
}
~~~

#### Property rules
Within the function it is possible to ensure the entity is only updated when a specific set of rules are passed. This can
be done by declaring the `property_rules` attribute on an item. The attribute is a list of different rule sets that function
as an `OR` query; at least one of the rule sets must be positive, otherwise the entity won't be updated. A rule set will
be positive if each rule within has a positive outcome when the conditions have met. These checks are only performed if 
all conditions are met.

A rule set can be defined as follow:
~~~json
{
  "property_rules": [
    {
      "conditions": [
        {
          "name": "old.test_field_1",
          "operator": "non_empty"
        }
      ],
      "rules": [
        {
          "name": "new.test_field_2",
          "operator": "greater_than",
          "value": "field:old.test_field_2"
        }
      ]
    }
  ]
}
~~~

Both the `conditions` and `rules` object consists of an endless list of rules where three fields per rule can be defined:

##### Name [required]
The `name` field is the key attribute that points to the value the rule is about. To ensure the value points towards
the correct field, the format defines which dataset must be used. By defining either `old.` or `new.` the field will be 
retrieved from either the existing entity or the new incoming message. After defining the dataset the name of the specific
field can be defined, e.g. `old.test_field_1`. This example will retrieve the value set for the field `test_field_1` from
the existing entity.

##### Operator [required]
The `operator` field will specify the condition the `name` value must abide to. The operators supported are:
- `non_empty`
- `empty`
- `is_true`
- `is_false`
- `contains`*
- `does_not_contain`*
- `equal_to`*
- `less_than`*
- `less_than_or_equal_to`*
- `greater_than`*
- `greater_than_or_equal_to`*

*\* Rule must also contain the [value](#value) field*

##### Value
The `value` is only required when using some [operators](#operator-[required]) (values with a `*`). This value field can be defined
as two types: a static value or a dynamic value. The dynamic value can be specified to point to a certain data field based
on the format described at '[Name](#name-[required])'. To use this dynamic field the format `field:<dataset>.<field_name>` 
can be used.

### Example
~~~python
AFAS_DB_PROCESSOR = {
    "employee": {
        "entity_name": "Employees",
        "id_property": "email_address"
    },
    "department": {
        "entity_name": "Departments",
        "id_property": "department_nr",
        "value_formatter": {
            "type": "split",
            "value": ".",
            "index": -1
        }
    },
    "bank": {
        "entity_name": "Employees",
        "filter_property": "employee_nr",
        "create_entity": True,
        "value_formatter": {
            "type": "prepend",
            "value": "test."
        }
    }
}
~~~

## License
This function is licensed under the [GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html) License

