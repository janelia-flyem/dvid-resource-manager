"""
JSON schemas for the messages sent between the client and the server.
"""

##
## Client -> Server
##

RequestMessageSchema = {
    "type": "object",
    "required": ["type", "resource", "read", "numopts", "datasize"],
    "properties": {
        "type": { "type": "string", "enum": ["request"] },
        "resource": { "type": "string" },
        "read": { "type": "boolean" },
        "numopts": { "type": "integer" },
        "datasize": { "type": "integer" }
    }
}

HoldMessageSchema = {
    "type": "object",
    "required": ["type", "id"],
    "properties": {
        "type": { "type": "string", "enum": ["hold"] },
        "id": { "type": "integer" },
    }
}

ReleaseMessageSchema = {
    "type": "object",
    "required": ["type", "id"],
    "properties": {
        "type": { "type": "string", "enum": ["release"] },
        "id": { "type": "integer" }
    }
}

ConfigSchema = {
    "type": "object",
    "additionalProperties": False,
    "required": ["read_reqs", "read_data", "write_reqs", "write_data"],
    "properties": {
        "read_reqs":  { "type": "integer" },
        "read_data":  { "type": "integer" },
        "write_reqs": { "type": "integer" },
        "write_data": { "type": "integer" }
    }
}

ConfigMessageSchema = {
    "required": ["type", "config"],
    "properties": {
        "type": { "type": "string", "enum": ["config"] },
        "config": ConfigSchema
    }
}

ReceivedMessageSchema = {
    "$schema": "http://json-schema.org/schema#",
    "title": "Request permission to access a resource",

    #"additionalProperties": False,

    # Message must be one of the above schemas.
    "oneOf": [ RequestMessageSchema,
               HoldMessageSchema,
               ReleaseMessageSchema,
               ConfigMessageSchema ]
}


##
## Server -> Client
##

RequestResponseSchema = {
    "$schema": "http://json-schema.org/schema#",
    "title": "Response to a 'request' release request",

    "type": "object",
    "additionalProperties": False,
    "required": ["id", "available"],
    "properties": {
        "id":  { "type": "integer" },
        "available":  { "type": "boolean" }
    }
}

# Response is just the new config, echoed back to the client.
ConfigResponseSchema = ConfigSchema

HoldResponseSchema = {
    "$schema": "http://json-schema.org/schema#",
    "title": "Response to a 'release' request",

    "type": "object",
    "additionalProperties": False,

    # Response is an empty object
    # (The act of sending the empty response tells the
    # client that the resource has been acquired.)
    "properties": {}
}

ReleaseResponseSchema = {
    "$schema": "http://json-schema.org/schema#",
    "title": "Response to a 'hold' request",

    "type": "object",
    "additionalProperties": False,

    # Response is an empty object.
    "properties": {}
}
