MESSAGE_PROPERTIES = {
    "main-message-field": {
        "entity_name": "main-message-field"
    }
}

# Name of the share storage where the file needs to go to
AZURE_DESTSHARE = "azure-destshare"
# Field in the published message from which the Azure sourcefilepath can be created
SOURCEPATH_FIELD = "published-message-field"

# The root field of the XML file
XML_ROOT = ""
# The first subelement of the root
XML_ROOT_SUBELEMENT = ""

# The mapping from the XML file fields to the field of the published message for Link2
MAPPING = {
    "field-in-link2-1": "published-message-field-1",
    "field-in-link2-2": "published-message-field-2",
    "field-in-link2-3": "published-message-field-3"
    # etcetera
}
