import jsonschema

request_schema = {
    "definitions": {},
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "http://example.com/root.json",
    "type": "object",
    "title": "The Root Schema",
    "required": [
        "StrategyOneRequest"
    ],
    "properties": {
        "StrategyOneRequest": {
            "$id": "#/properties/StrategyOneRequest",
            "type": "object",
            "title": "The Strategyonerequest Schema",
            "required": [
                "Header",
                "Body"
            ],
            "properties": {
                "Header": {
                    "$id": "#/properties/StrategyOneRequest/properties/Header",
                    "type": "object",
                    "title": "The Header Schema",
                    "required": [
                        "InquiryCode",
                        "ProcessCode"
                    ],
                    "properties": {
                        "InquiryCode": {
                            "$id": "#/properties/StrategyOneRequest/properties/Header/properties/InquiryCode",
                            "type": "string",
                            "title": "The Inquirycode Schema",
                            "default": "",
                            "examples": [
                                "c3ef30f0ad5646d8a25136f98532ec9f"
                            ],
                            "pattern": "^(.*)$"
                        },
                        "ProcessCode": {
                            "$id": "#/properties/StrategyOneRequest/properties/Header/properties/ProcessCode",
                            "type": "string",
                            "title": "The Processcode Schema",
                            "default": "",
                            "examples": [
                                "JB_WZ_CJR2"
                            ],
                            "pattern": "^(.*)$"
                        }
                    }
                },
                "Body": {
                    "$id": "#/properties/StrategyOneRequest/properties/Body",
                    "type": "object",
                    "title": "The Body Schema",
                    "required": [
                        "Application"
                    ],
                    "properties": {
                        "Application": {
                            "$id": "#/properties/StrategyOneRequest/properties/Body/properties/Application",
                            "type": "object",
                            "title": "The Application Schema",
                            "required": [
                                "Variables"
                            ],
                            "properties": {
                                "Variables": {
                                    "$id": "#/properties/StrategyOneRequest/properties/Body/properties/Application/properties/Variables",
                                    "type": "object",
                                    "title": "The Variables Schema",
                                    "required": [
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


def validate_input(input):
    jsonschema.validate(input, request_schema)
