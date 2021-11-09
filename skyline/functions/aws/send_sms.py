import logging
import traceback

import boto3


# @added 20210724 - Feature #4196: functions.aws.send_sms
def send_sms(current_skyline_app, number, message):

    function_str = 'functions.aws.send_sms'
    current_skyline_app_logger = current_skyline_app + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    response = None
    success = False
    try:
        # Create an SNS client
        client = boto3.client('sns')
        # client = boto3.client(
        #     'sns', aws_access_key_id=settings.AWS_OPTS['aws_access_key_id'],
        #     aws_secret_access_key=settings.AWS_OPTS['aws_secret_access_key'],
        #     region_name=settings.AWS_OPTS['region_name']
        # )
        # Send your sms message.
        response = client.publish(
            PhoneNumber=str(number), Message=message,
            MessageAttributes={
                'AWS.SNS.SMS.SMSType': {
                    'DataType': 'String',
                    'StringValue': 'Transactional',
                }
            }
        )
    except Exception as e:
        current_logger.error(traceback.format_exc())
        current_logger.error('error :: %s :: failed to send SMS message to %s, %s - %s' % (
            function_str, number, message, e))
    if response:
        current_logger.info('%s :: sent SMS message to %s, %s - AWS response: %s' % (
            function_str, number, message, str(response)))
        try:
            status_code = response['ResponseMetadata']['HTTPStatusCode']
            if status_code == 200:
                success = True
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: %s :: failed to determine if SMS was sent for %s, %s - %s' % (
                function_str, number, message, e))
    else:
        current_logger.warn('warning :: %s :: unkown SMS message status to %s, %s - AWS response: %s' % (
            function_str, number, message, str(response)))

    return success, response
