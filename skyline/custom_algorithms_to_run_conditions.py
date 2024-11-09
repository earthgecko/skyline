"""
custom_algorithms_to_run_conditions.py
"""


# @added 20240717 - Feature #5390: custom_algorithms - condition
def current_value_below(current_skyline_app, target_value, timeseries):
    """
    Return True if the condition is met
    """
    result = False
    try:
        value = timeseries[-1][1]
        if isinstance(value, float):
            if value < target_value:
                result = True
    except:
        result = False
    return result
