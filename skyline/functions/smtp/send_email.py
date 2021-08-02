import logging
import traceback
from smtplib import SMTP
from email import charset
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import settings


# @added 20210524 - Branch #1444: thunder
def send_email(current_skyline_app, to, cc, subject, body, log=True):
    """
    Send a plain text email.

    :param current_skyline_app: the app calling the function
    :param to: the to address
    :param cc: the cc addresses
    :param subject: the subject
    :param body: the body
    :type current_skyline_app: str
    :type to: str
    :type cc: list
    :type body: str
    :type log: boolean
    :return: sent
    :rtype: boolean

    """

    function_str = 'functions.smtp.send_email'
    if log:
        current_skyline_app_logger = current_skyline_app + 'Log'
        current_logger = logging.getLogger(current_skyline_app_logger)
    else:
        current_logger = None

    sender = settings.SMTP_OPTS['sender']
    primary_recipient = to
    cc_recipients = False
    recipients = [to]
    current_logger.info(
        '%s - will send to primary_recipient :: %s, cc :: %s' %
        (function_str, str(primary_recipient), str(cc)))
    if len(cc) > 0:
        recipients = recipients + cc

    if recipients:
        for i_recipient in recipients:
            if not primary_recipient:
                primary_recipient = str(i_recipient)
            if primary_recipient != i_recipient:
                if not cc_recipients:
                    cc_recipients = str(i_recipient)
                else:
                    new_cc_recipients = '%s,%s' % (str(cc_recipients), str(i_recipient))
                    cc_recipients = str(new_cc_recipients)
    current_logger.info(
        '%s - will send to primary_recipient :: %s, cc_recipients :: %s' %
        (function_str, str(primary_recipient), str(cc_recipients)))

    send_email_alert = True
    if primary_recipient:
        try:
            msg = MIMEMultipart('mixed')
            cs_ = charset.Charset('utf-8')
            cs_.header_encoding = charset.QP
            cs_.body_encoding = charset.QP
            msg.set_charset(cs_)

            msg['Subject'] = str(subject)
            msg['From'] = sender
            msg['To'] = primary_recipient
            if cc_recipients:
                msg['Cc'] = cc_recipients

            html_body = '<h3><font color="#dd3023">Sky</font><font color="#6698FF">line</font><font color="black"></font></h3><br>'
            html_body += '%s' % str(body)
            # msg.attach(MIMEText(str(body), 'text'))
            msg.attach(MIMEText(html_body, 'html'))

            msg.replace_header('content-transfer-encoding', 'quoted-printable')

            if settings.DOCKER_FAKE_EMAIL_ALERTS:
                current_logger.info('alert_smtp - DOCKER_FAKE_EMAIL_ALERTS is set to %s, not executing SMTP command' % str(settings.DOCKER_FAKE_EMAIL_ALERTS))
                send_email_alert = False
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error(
                'error :: %s :: could not send email to primary_recipient :: %s, cc_recipients :: %s - %s' %
                (str(function_str), str(primary_recipient), str(cc_recipients), e))
            send_email_alert = False

    if send_email_alert:
        try:
            s = SMTP('127.0.0.1')
            if cc_recipients:
                s.sendmail(sender, [primary_recipient, cc_recipients], msg.as_string())
            else:
                s.sendmail(sender, primary_recipient, msg.as_string())
            current_logger.info('%s :: email sent - %s - primary_recipient :: %s, cc_recipients :: %s' % (
                str(function_str), subject, str(primary_recipient), str(cc_recipients)))
        except Exception as e:
            current_logger.error(traceback.format_exc())
            current_logger.error(
                'error :: %s :: could not send email to primary_recipient :: %s, cc_recipients :: %s - %s' % (
                    str(function_str), str(primary_recipient), str(cc_recipients), e))
        s.quit()
    else:
        current_logger.info(
            'alert_smtp - send_email_alert was set to %s message was not sent to primary_recipient :: %s, cc_recipients :: %s' % (
                str(send_email_alert), str(primary_recipient), str(cc_recipients)))

    return
