# Afas consume

This functions consumes Pub/Sub messages from an Afas instance and is based on configuration.

## Configuration
A `config.py` file is used to determine what the function should do with the incoming message. 
See [config.py.example](config.py.example) for an example. 

### Data Selector
A special Data Selector specifies which fields have to be processed. Each attribute specifies an entity to be updated 
through Pub/Sub messages. The configuration to use is specified by the `DATA_SELECTOR` environment variable.

#### Entity name
The `entity_name` specifies the DataStore Entity kind to be created/updated.

#### ID property
When `id_property` is specified, the entity is retrieved based on key retrieval, with the value set to the specified attribute.
The entity will be created when it does not exist.

#### Filter property
When `filter_property` is specified, the entity is retrieved based on a filter on the specified attribute, comparing to
the value retrieved from the message received. Using `filter_property` only supports updated and won't create new entities.

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

### Domain Validation Token
A `DOMAIN_VALIDATION_TOKEN` is used to validate the function as a verified domain to enable pub/sub pushConfig.

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
    }
}

DOMAIN_VALIDATION_TOKEN = 'b2ba11bd-acaf-4128-92b8-ac8138a41cae'
~~~

## License
This function is licensed under the [GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html) License

