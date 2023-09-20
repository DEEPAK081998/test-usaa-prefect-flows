

class EmailConfig:
    def __init__(self, kwargs: dict, partner_email_variables: dict = {}) -> None:
        self.s3_bucket_name: str = partner_email_variables['s3_bucket_name']
        self.email_subject: str = kwargs['email_subject']
        self.email_tag: str = kwargs['email_tag']
        self.manifest_path: str = kwargs['manifest_path']
        self.html_template_name: str = kwargs.get('html_template_name')
        self.report_type: str = kwargs['report_type']
        self.emails: str = kwargs.get('emails')
        self.replacements: str = kwargs.get('replacements')
        self.email_cc: str = kwargs.get('email_cc')
        self.email_from: str = kwargs['email_from']
        self.reply_to: str = kwargs.get('reply_to')
        self.signature: str = kwargs.get('signature')
        self.email_to: str = kwargs.get('email_to')
        self.generation: dict = kwargs.get('generation', dict())
        self.required_columns: list = kwargs.get('required_columns')
        self.attachment_name_template: str = kwargs.get('attachment_name_template')
        self.columns_order: str = kwargs.get('columns_order')
        self.send_to: str = kwargs.get('send_to')  # REP-1775

    def __str__(self) -> str:
        """Return object attributes when printing it."""
        return str(self.__dict__)

    def __repr__(self) -> str:
        """Return object attributes when printing it."""
        return str(self.__dict__)


def read_email_configuration(partner_email_kwargs, partner_email_variables) -> EmailConfig:
    """Return report configuration dict from YAML file.

    Return:
        - email_config: Email configuration dictionary
    """

    email_config = EmailConfig(partner_email_kwargs, partner_email_variables)
    return email_config
