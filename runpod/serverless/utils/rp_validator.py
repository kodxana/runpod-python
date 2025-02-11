'''
runpod | serverless | utils | validator.py
Provides a function to validate the input to the model.
'''
# pylint: disable=too-many-branches

import json
from typing import Any, Dict, List, Union

# Error messages
UNEXPECTED_INPUT_ERROR = "Unexpected input. {} is not a valid input option."
MISSING_REQUIRED_ERROR = "{} is a required input."
MISSING_DEFAULT_ERROR = "Schema error, missing default value for {}."
MISSING_TYPE_ERROR = "Schema error, missing type for {}."
INVALID_TYPE_ERROR = "{} should be {} type, not {}."
CONSTRAINTS_ERROR = "{} does not meet the constraints."
SCHEMA_ERROR = "Schema error, {} is not a dictionary."


def _add_error(error_list: List[str], message: str) -> None:
    error_list.append(message)


def validate(raw_input: Dict[str, Any], schema: Dict[str, Any]
             ) -> Dict[str, Union[Dict[str, Any], List[str]]]:
    '''
    Validates the input.
    Checks to see if the provided inputs match the expected types.
    Checks to see if the required inputs are included.
    Sets the default values for the inputs that are not provided.
    Validates the inputs using the lambda constraints.

    Returns either the list of errors or a validated_job_input.
    {"errors": ["error1", "error2"]}
    or
    {"validated_input": {"input1": "value1", "input2": "value2"}
    '''
    error_list = []
    validated_input = raw_input.copy()

    # Check for unexpected inputs.
    for key in raw_input:
        if key not in schema:
            _add_error(error_list, UNEXPECTED_INPUT_ERROR.format(key))

    # Check that items are dictionaries.
    for key, rules in schema.items():
        if not isinstance(rules, dict):
            try:
                schema[key] = json.loads(rules)
            except json.decoder.JSONDecodeError:
                _add_error(error_list, SCHEMA_ERROR.format(key))

    # Checks for missing required inputs or sets the default values.
    for key, rules in schema.items():
        if 'type' not in rules:
            _add_error(error_list, MISSING_TYPE_ERROR.format(key))

        if 'required' not in rules:
            _add_error(error_list, MISSING_REQUIRED_ERROR.format(key))
        elif rules['required'] and key not in raw_input:
            _add_error(error_list, MISSING_REQUIRED_ERROR.format(key))
        elif rules['required'] and key not in raw_input and "default" not in rules:
            _add_error(error_list, MISSING_DEFAULT_ERROR.format(key))
        elif not rules['required'] and key not in raw_input and "default" not in rules:
            _add_error(error_list, MISSING_DEFAULT_ERROR.format(key))
        elif not rules['required'] and key not in raw_input:
            validated_input[key] = raw_input.get(key, rules['default'])

    for key, rules in schema.items():
        # Enforce floats to be floats.
        if rules['type'] is float and type(raw_input[key]) in [int, float]:
            validated_input[key] = float(raw_input[key])

        # Check for the correct type.
        if not isinstance(raw_input[key], rules['type']) and raw_input[key] is not None:
            _add_error(
                error_list, f"{key} should be {rules['type']} type, not {type(raw_input[key])}.")

        # Check lambda constraints.
        if "constraints" in rules:
            if not rules['constraints'](raw_input[key]):
                _add_error(error_list, CONSTRAINTS_ERROR.format(key))

    validation_return = {"validated_input": validated_input}
    if error_list:
        validation_return = {"errors": error_list}

    return validation_return
